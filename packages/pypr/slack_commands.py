from __future__ import annotations

import json
import shlex
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

READ_ONLY_COMMANDS = {
    "/help",
    "/health",
    "/policy",
    "/state",
    "/timeline",
    "/memory",
}


class SlackCommandRouter:
    def __init__(self, base_url: str, read_only: bool = True) -> None:
        self.base_url = base_url.rstrip("/")
        self.read_only = read_only

    def run_command(self, raw: str) -> dict[str, Any]:
        raw = raw.strip()
        if not raw:
            return {"ok": False, "error": "empty command"}

        try:
            parts = shlex.split(raw)
        except ValueError as exc:
            return {"ok": False, "error": f"parse error: {exc}"}

        cmd = parts[0]
        args = parts[1:]

        if self.read_only and cmd not in READ_ONLY_COMMANDS:
            return {"ok": False, "error": f"read-only mode: command not allowed: {cmd}"}

        if cmd == "/help":
            return {
                "ok": True,
                "data": {
                    "commands": [
                        "/help",
                        "/health",
                        "/policy",
                        "/state <customer_id>",
                        "/timeline <customer_id> [limit]",
                        "/memory [kind=<kind>] [tag=<tag>] [key_prefix=<prefix>] [min_conf=<0..1>] [limit=<n>]",
                    ]
                },
            }

        if cmd == "/health":
            return {"ok": True, "data": self._http_get("/health")}

        if cmd == "/policy":
            return {"ok": True, "data": self._http_get("/v1/policy")}

        if cmd == "/state":
            if len(args) != 1:
                return {"ok": False, "error": "usage: /state <customer_id>"}
            customer_id = args[0]
            return {"ok": True, "data": self._http_get(f"/v1/customers/{customer_id}/state")}

        if cmd == "/timeline":
            if len(args) < 1 or len(args) > 2:
                return {"ok": False, "error": "usage: /timeline <customer_id> [limit]"}
            customer_id = args[0]
            limit = args[1] if len(args) == 2 else "20"
            if not limit.isdigit():
                return {"ok": False, "error": "limit must be an integer"}
            query = urllib.parse.urlencode({"limit": int(limit)})
            path = f"/v1/customers/{customer_id}/timeline?{query}"
            return {"ok": True, "data": self._http_get(path)}

        if cmd == "/memory":
            payload: dict[str, Any] = {"limit": 20}
            for token in args:
                if "=" not in token:
                    return {"ok": False, "error": f"invalid filter token: {token}"}
                key, value = token.split("=", 1)
                key = key.strip()
                value = value.strip()

                if key in {"kind", "tag", "key_prefix"}:
                    payload[key] = value
                elif key == "limit":
                    if not value.isdigit():
                        return {"ok": False, "error": "limit must be an integer"}
                    payload["limit"] = int(value)
                elif key == "min_conf":
                    try:
                        payload["min_confidence"] = float(value)
                    except ValueError:
                        return {"ok": False, "error": "min_conf must be a float"}
                else:
                    return {"ok": False, "error": f"unsupported filter: {key}"}

            return {"ok": True, "data": self._http_post("/v1/memory/search", payload)}

        return {"ok": False, "error": f"unknown command: {cmd}"}

    def _http_get(self, path: str) -> Any:
        req = urllib.request.Request(f"{self.base_url}{path}", method="GET")
        return self._read_json(req)

    def _http_post(self, path: str, payload: dict[str, Any]) -> Any:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            method="POST",
            headers={"content-type": "application/json"},
        )
        return self._read_json(req)

    @staticmethod
    def _read_json(req: urllib.request.Request) -> Any:
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8") if exc.fp else ""
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"network error: {exc.reason}") from exc
