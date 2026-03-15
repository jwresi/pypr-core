#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import traceback
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


TOOLS = [
    {
        "name": "get_server_info",
        "description": "Return NetBox MCP configuration status and basic diagnostics.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_objects",
        "description": "Fetch NetBox core objects by type and optional filters.",
        "inputSchema": {
            "type": "object",
            "required": ["object_type"],
            "properties": {
                "object_type": {"type": "string"},
                "filters": {"type": "object"},
                "limit": {"type": "integer", "default": 50},
                "offset": {"type": "integer", "default": 0},
                "fields": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "get_object_by_id",
        "description": "Fetch a single NetBox object by type and ID.",
        "inputSchema": {
            "type": "object",
            "required": ["object_type", "id"],
            "properties": {
                "object_type": {"type": "string"},
                "id": {"type": "integer"},
                "fields": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "get_changelogs",
        "description": "Fetch NetBox object change records with optional filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "filters": {"type": "object"},
                "limit": {"type": "integer", "default": 50},
                "offset": {"type": "integer", "default": 0},
            },
        },
    },
]


CORE_OBJECT_PATHS = {
    "devices": "/api/dcim/devices/",
    "sites": "/api/dcim/sites/",
    "device-types": "/api/dcim/device-types/",
    "interfaces": "/api/dcim/interfaces/",
    "cables": "/api/dcim/cables/",
    "manufacturers": "/api/dcim/manufacturers/",
    "platforms": "/api/dcim/platforms/",
    "racks": "/api/dcim/racks/",
    "locations": "/api/dcim/locations/",
    "regions": "/api/dcim/regions/",
    "ip-addresses": "/api/ipam/ip-addresses/",
    "prefixes": "/api/ipam/prefixes/",
    "vlans": "/api/ipam/vlans/",
    "vrfs": "/api/ipam/vrfs/",
    "asns": "/api/ipam/asns/",
    "l2vpn-terminations": "/api/vpn/l2vpn-terminations/",
    "l2vpns": "/api/vpn/l2vpns/",
    "wireless-lans": "/api/wireless/wireless-lans/",
    "wireless-links": "/api/wireless/wireless-links/",
    "circuits": "/api/circuits/circuits/",
}


class NetBoxClient:
    def __init__(self) -> None:
        self.url = os.environ.get("NETBOX_URL", "").rstrip("/")
        self.token = os.environ.get("NETBOX_TOKEN", "")
        self.verify_ssl = os.environ.get("VERIFY_SSL", "true").lower() not in {"0", "false", "no"}

    def configured(self) -> bool:
        return bool(self.url and self.token)

    def request(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.configured():
            raise ValueError("NETBOX_URL or NETBOX_TOKEN is not configured")
        query = urllib.parse.urlencode(_flatten_params(params or {}), doseq=True)
        full_url = f"{self.url}{path}"
        if query:
            full_url = f"{full_url}?{query}"
        req = urllib.request.Request(
            full_url,
            headers={
                "Authorization": f"Token {self.token}",
                "Accept": "application/json",
            },
        )
        context = None
        if not self.verify_ssl:
            import ssl

            context = ssl._create_unverified_context()
        try:
            with urllib.request.urlopen(req, context=context, timeout=20) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"NetBox HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"NetBox connection error: {exc}") from exc
        return json.loads(body)

    def probe_status(self) -> dict[str, Any]:
        if not self.configured():
            return {"ok": False, "error": "NETBOX_URL or NETBOX_TOKEN is not configured"}
        try:
            payload = self.request("/api/status/")
            return {"ok": True, "status": payload}
        except Exception as exc:
            msg = str(exc)
            diagnostic = {"ok": False, "error": msg}
            if "cf-mitigated" in msg or "Just a moment" in msg or "Cloudflare" in msg:
                diagnostic["likely_blocker"] = "cloudflare_challenge"
            return diagnostic


def _flatten_params(params: dict[str, Any]) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, list):
            flat[key] = value
        elif isinstance(value, dict):
            flat[key] = json.dumps(value)
        else:
            flat[key] = value
    return flat


class MCPServer:
    def __init__(self) -> None:
        self.client = NetBoxClient()

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
                    "serverInfo": {"name": "netbox-readonly-mcp", "version": "0.1.0"},
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
            return {"content": [{"type": "text", "text": json.dumps(self._server_info())}]}
        if name == "get_objects":
            text = json.dumps(self._get_objects(arguments))
            return {"content": [{"type": "text", "text": text}]}
        if name == "get_object_by_id":
            text = json.dumps(self._get_object_by_id(arguments))
            return {"content": [{"type": "text", "text": text}]}
        if name == "get_changelogs":
            text = json.dumps(self._get_changelogs(arguments))
            return {"content": [{"type": "text", "text": text}]}
        raise ValueError(f"Unknown tool: {name}")

    def _server_info(self) -> dict[str, Any]:
        return {
            "configured": self.client.configured(),
            "netbox_url": self.client.url,
            "verify_ssl": self.client.verify_ssl,
            "supported_object_types": sorted(CORE_OBJECT_PATHS.keys()),
            "tools": [tool["name"] for tool in TOOLS],
            "connectivity_probe": self.client.probe_status(),
        }

    def _get_objects(self, arguments: dict[str, Any]) -> dict[str, Any]:
        object_type = arguments["object_type"]
        path = CORE_OBJECT_PATHS.get(object_type)
        if not path:
            raise ValueError(f"Unsupported object_type: {object_type}")
        filters = dict(arguments.get("filters") or {})
        filters["limit"] = int(arguments.get("limit", 50))
        filters["offset"] = int(arguments.get("offset", 0))
        fields = arguments.get("fields")
        if fields:
            filters["brief"] = 1
        result = self.client.request(path, filters)
        if fields and "results" in result:
            result["results"] = [{key: item.get(key) for key in fields} for item in result["results"]]
        return result

    def _get_object_by_id(self, arguments: dict[str, Any]) -> dict[str, Any]:
        object_type = arguments["object_type"]
        obj_id = int(arguments["id"])
        path = CORE_OBJECT_PATHS.get(object_type)
        if not path:
            raise ValueError(f"Unsupported object_type: {object_type}")
        result = self.client.request(f"{path}{obj_id}/")
        fields = arguments.get("fields")
        if fields:
            result = {key: result.get(key) for key in fields}
        return result

    def _get_changelogs(self, arguments: dict[str, Any]) -> dict[str, Any]:
        filters = dict(arguments.get("filters") or {})
        filters["limit"] = int(arguments.get("limit", 50))
        filters["offset"] = int(arguments.get("offset", 0))
        return self.client.request("/api/core/object-changes/", filters)

    def _read_message(self) -> dict[str, Any] | None:
        header_line = sys.stdin.buffer.readline()
        if not header_line:
            return None
        header = header_line.decode("utf-8").strip()
        if not header.lower().startswith("content-length:"):
            raise ValueError(f"Invalid header: {header}")
        content_length = int(header.split(":", 1)[1].strip())
        while True:
            line = sys.stdin.buffer.readline()
            if not line or line in (b"\r\n", b"\n"):
                break
        body = sys.stdin.buffer.read(content_length)
        return json.loads(body.decode("utf-8"))

    def _write_message(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message).encode("utf-8")
        sys.stdout.buffer.write(f"Content-Length: {len(payload)}\r\n\r\n".encode("utf-8"))
        sys.stdout.buffer.write(payload)
        sys.stdout.buffer.flush()


def main() -> None:
    MCPServer().run()


if __name__ == "__main__":
    main()
