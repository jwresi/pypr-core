#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import traceback
import urllib.error
import urllib.request
from typing import Any

TOOLS = [
    {
        "name": "get_server_info",
        "description": "Return cnWave exporter MCP configuration status and connectivity diagnostics.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_metrics_summary",
        "description": "Return summary counts from cnWave Prometheus metrics, optionally filtered by site_id.",
        "inputSchema": {"type": "object", "properties": {"site_id": {"type": "string"}}},
    },
    {
        "name": "get_device_status",
        "description": "Return cnWave device status metric rows, optionally filtered by site_id or device name.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "site_id": {"type": "string"},
                "name": {"type": "string"},
            },
        },
    },
    {
        "name": "get_link_issues",
        "description": "Return cnWave links that are down or unhealthy, optionally filtered by site_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "site_id": {"type": "string"},
                "down_only": {"type": "boolean", "default": True},
            },
        },
    },
]

METRIC_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)$")
LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse_prometheus_text(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = METRIC_RE.match(line)
        if not match:
            continue
        name, label_blob, value = match.groups()
        labels: dict[str, str] = {}
        if label_blob:
            for key, label_value in LABEL_RE.findall(label_blob):
                labels[key] = bytes(label_value, "utf-8").decode("unicode_escape")
        numeric = float(value)
        rows.append(
            {
                "name": name,
                "labels": labels,
                "value": int(numeric) if numeric.is_integer() else numeric,
            }
        )
    return rows


def filter_rows(rows: list[dict[str, Any]], site_id: str | None = None, name: str | None = None) -> list[dict[str, Any]]:
    scoped = rows
    if site_id:
        scoped = [row for row in scoped if str(row.get("labels", {}).get("site_id", "")) == str(site_id)]
    if name:
        needle = str(name).lower()
        scoped = [row for row in scoped if needle in str(row.get("labels", {}).get("name", "")).lower()]
    return scoped


class CnwaveExporterClient:
    def __init__(self) -> None:
        self.url = os.environ.get("CNWAVE_EXPORTER_URL", "").rstrip("/")

    def configured(self) -> bool:
        return bool(self.url)

    def fetch_metrics_text(self) -> str:
        if not self.configured():
            raise ValueError("CNWAVE_EXPORTER_URL is not configured")
        req = urllib.request.Request(f"{self.url}/metrics", headers={"Accept": "text/plain"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"cnWave exporter HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"cnWave exporter connection error: {exc}") from exc

    def metrics(self) -> list[dict[str, Any]]:
        return parse_prometheus_text(self.fetch_metrics_text())

    def probe(self) -> dict[str, Any]:
        if not self.configured():
            return {"ok": False, "error": "CNWAVE_EXPORTER_URL is not configured"}
        try:
            rows = self.metrics()
            return {"ok": True, "metric_rows": len(rows)}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}


def summarize_metrics(rows: list[dict[str, Any]], site_id: str | None = None) -> dict[str, Any]:
    scoped = filter_rows(rows, site_id=site_id)
    device_status = [r for r in scoped if r["name"] == "cnwave_device_status"]
    link_status = [r for r in scoped if r["name"] == "cnwave_link_status"]
    device_alarms = [r for r in scoped if r["name"] == "cnwave_device_alarms"]
    return {
        "site_id": site_id,
        "device_rows": len(device_status),
        "device_up": sum(1 for r in device_status if float(r["value"]) >= 1),
        "device_down": sum(1 for r in device_status if float(r["value"]) < 1),
        "link_rows": len(link_status),
        "link_up": sum(1 for r in link_status if float(r["value"]) >= 1),
        "link_down": sum(1 for r in link_status if float(r["value"]) < 1),
        "alarm_rows": len(device_alarms),
        "alarm_total": sum(int(float(r["value"])) for r in device_alarms),
    }


class MCPServer:
    def __init__(self) -> None:
        self.client = CnwaveExporterClient()

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
                    "serverInfo": {"name": "cnwave-exporter-readonly-mcp", "version": "0.1.0"},
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
                    "cnwave_exporter_url": self.client.url,
                    "tools": [tool["name"] for tool in TOOLS],
                    "connectivity_probe": self.client.probe(),
                }
            )
            return {"content": [{"type": "text", "text": text}]}
        rows = self.client.metrics()
        if name == "get_metrics_summary":
            return {"content": [{"type": "text", "text": json.dumps(summarize_metrics(rows, arguments.get("site_id")))}]}
        if name == "get_device_status":
            scoped = [r for r in filter_rows(rows, site_id=arguments.get("site_id"), name=arguments.get("name")) if r["name"] == "cnwave_device_status"]
            return {"content": [{"type": "text", "text": json.dumps(scoped)}]}
        if name == "get_link_issues":
            scoped = [r for r in filter_rows(rows, site_id=arguments.get("site_id")) if r["name"] == "cnwave_link_status"]
            if arguments.get("down_only", True):
                scoped = [r for r in scoped if float(r["value"]) < 1]
            return {"content": [{"type": "text", "text": json.dumps(scoped)}]}
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
