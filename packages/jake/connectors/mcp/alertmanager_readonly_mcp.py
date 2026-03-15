#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import traceback
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


TOOLS = [
    {
        "name": "get_server_info",
        "description": "Return Alertmanager MCP configuration status and basic diagnostics.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_alerts",
        "description": "Fetch active alerts with optional filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "active": {"type": "boolean", "default": True},
                "silenced": {"type": "boolean", "default": False},
                "inhibited": {"type": "boolean", "default": False},
                "unprocessed": {"type": "boolean", "default": False},
                "filter": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "get_status",
        "description": "Fetch Alertmanager status/config summary.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "summarize_alerts",
        "description": "Return alert counts by severity, alertname, and site_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "active": {"type": "boolean", "default": True},
                "filter": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
]


class AlertmanagerClient:
    def __init__(self) -> None:
        self.url = os.environ.get("ALERTMANAGER_URL", "").rstrip("/")

    def configured(self) -> bool:
        return bool(self.url)

    def request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self.configured():
            raise ValueError("ALERTMANAGER_URL is not configured")
        full_url = f"{self.url}{path}"
        if params:
            full_url = f"{full_url}?{urllib.parse.urlencode(params, doseq=True)}"
        req = urllib.request.Request(full_url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Alertmanager HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Alertmanager connection error: {exc}") from exc
        return json.loads(body)

    def probe(self) -> dict[str, Any]:
        if not self.configured():
            return {"ok": False, "error": "ALERTMANAGER_URL is not configured"}
        try:
            data = self.request("/api/v2/status")
            return {"ok": True, "cluster": data.get("cluster", {})}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}


class MCPServer:
    def __init__(self) -> None:
        self.client = AlertmanagerClient()

    def run(self) -> None:
        while True:
            message = self._read_message()
            if message is None:
                return
            if "method" in message and message.get("id") is None:
                continue
            self._handle_request(message)

    def _handle_request(self, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        method = message.get("method")
        try:
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "alertmanager-readonly-mcp", "version": "0.1.0"},
                }
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = {"tools": TOOLS}
            elif method == "tools/call":
                result = self._call_tool(message.get("params", {}))
            else:
                raise ValueError(f"Unsupported method: {method}")
            self._write_message({"jsonrpc": "2.0", "id": request_id, "result": result})
        except Exception as exc:
            self._write_message(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32000, "message": str(exc), "data": traceback.format_exc()},
                }
            )

    def _call_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if name == "get_server_info":
            text = json.dumps(
                {
                    "configured": self.client.configured(),
                    "alertmanager_url": self.client.url,
                    "tools": [tool["name"] for tool in TOOLS],
                    "connectivity_probe": self.client.probe(),
                }
            )
            return {"content": [{"type": "text", "text": text}]}
        if name == "get_alerts":
            query = {
                "active": str(arguments.get("active", True)).lower(),
                "silenced": str(arguments.get("silenced", False)).lower(),
                "inhibited": str(arguments.get("inhibited", False)).lower(),
                "unprocessed": str(arguments.get("unprocessed", False)).lower(),
                "filter": arguments.get("filter") or [],
            }
            return {"content": [{"type": "text", "text": json.dumps(self.client.request("/api/v2/alerts", query))}]}
        if name == "get_status":
            return {"content": [{"type": "text", "text": json.dumps(self.client.request("/api/v2/status"))}]}
        if name == "summarize_alerts":
            query = {
                "active": str(arguments.get("active", True)).lower(),
                "filter": arguments.get("filter") or [],
            }
            alerts = self.client.request("/api/v2/alerts", query)
            summary: dict[str, Any] = {
                "total": len(alerts),
                "by_severity": {},
                "by_alertname": {},
                "by_site_id": {},
            }
            for alert in alerts:
                labels = alert.get("labels", {})
                sev = labels.get("severity", "unknown")
                name = labels.get("alertname", "unknown")
                site = labels.get("site_id", "unknown")
                summary["by_severity"][sev] = summary["by_severity"].get(sev, 0) + 1
                summary["by_alertname"][name] = summary["by_alertname"].get(name, 0) + 1
                summary["by_site_id"][site] = summary["by_site_id"].get(site, 0) + 1
            return {"content": [{"type": "text", "text": json.dumps(summary)}]}
        raise ValueError(f"Unknown tool: {name}")

    def _read_message(self) -> dict[str, Any] | None:
        line = input()
        if not line:
            return None
        return json.loads(line)

    def _write_message(self, message: dict[str, Any]) -> None:
        print(json.dumps(message), flush=True)


if __name__ == "__main__":
    MCPServer().run()
