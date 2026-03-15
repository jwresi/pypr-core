#!/usr/bin/env python3
from __future__ import annotations

import json
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.jake.connectors.mcp.jake_ops_mcp import JakeOps  # noqa: E402

TOOLS = [
    {
        "name": "query_summary",
        "description": "Primary Jake front door. Accept a normal network operations question and return the deterministic Jake answer with matched action, operator summary, and raw result.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string"},
            },
        },
    },
    {
        "name": "get_server_info",
        "description": "Return Jake Front Door MCP status.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


class JakeFrontDoor:
    def __init__(self) -> None:
        self.ops = JakeOps()

    def get_server_info(self) -> dict[str, Any]:
        return {
            "name": "jake-frontdoor-mcp",
            "version": "0.1.0",
            "tool_count": len(TOOLS),
            "tools": [t["name"] for t in TOOLS],
            "backing_server": "jake_ops_mcp",
        }

    def query_summary(self, query: str) -> dict[str, Any]:
        return self.ops.query_summary(query)


class Server:
    def __init__(self) -> None:
        self.impl = JakeFrontDoor()

    def handle(self, req: dict[str, Any]) -> dict[str, Any] | None:
        method = req.get("method")
        req_id = req.get("id")
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "jake-frontdoor-mcp", "version": "0.1.0"},
                },
            }
        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
        if method == "tools/call":
            params = req.get("params", {})
            name = params.get("name")
            args = params.get("arguments", {})
            if name == "get_server_info":
                data = self.impl.get_server_info()
            elif name == "query_summary":
                data = self.impl.query_summary(args["query"])
            else:
                raise ValueError(f"Unknown tool: {name}")
            return {"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "text", "text": json.dumps(data)}]}}
        if method == "notifications/initialized":
            return None
        raise ValueError(f"Unknown method: {method}")


def main() -> None:
    server = Server()
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            resp = server.handle(req)
            if resp is not None:
                sys.stdout.write(json.dumps(resp) + "\n")
                sys.stdout.flush()
        except Exception as exc:
            err = {
                "jsonrpc": "2.0",
                "id": req.get("id") if 'req' in locals() and isinstance(req, dict) else None,
                "error": {"code": -32000, "message": str(exc), "data": traceback.format_exc()},
            }
            sys.stdout.write(json.dumps(err) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
