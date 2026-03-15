#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import ssl
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any


TOOLS = [
    {
        "name": "get_server_info",
        "description": "Return TAUC MCP configuration status, auth mode, TLS diagnostics, and write-policy state.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "cloud_get_network_name_list",
        "description": "Fetch TAUC cloud network names by status such as ONLINE or ABNORMAL.",
        "inputSchema": {
            "type": "object",
            "required": ["status"],
            "properties": {
                "status": {"type": "string", "enum": ["ONLINE", "ABNORMAL"]},
                "page": {"type": "integer", "default": 0},
                "page_size": {"type": "integer", "default": 100},
            },
        },
    },
    {
        "name": "cloud_get_network_details",
        "description": "Fetch TAUC cloud network-system-management details for one network.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {"network_id": {"type": "string"}},
        },
    },
    {
        "name": "cloud_get_network",
        "description": "Fetch TAUC cloud network object by network id.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {"network_id": {"type": "string"}},
        },
    },
    {
        "name": "cloud_get_preconfiguration_status",
        "description": "Fetch Aginet preconfiguration status for one network.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {"network_id": {"type": "string"}},
        },
    },
    {
        "name": "cloud_get_pppoe_status",
        "description": "Fetch PPPoE configured-status for one network, optionally including current credentials.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {
                "network_id": {"type": "string"},
                "refresh": {"type": "boolean", "default": True},
                "include_credentials": {"type": "boolean", "default": False},
            },
        },
    },
    {
        "name": "cloud_get_pppoe_credentials",
        "description": "Fetch PPPoE credentials for one network.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {
                "network_id": {"type": "string"},
                "refresh": {"type": "boolean", "default": True},
            },
        },
    },
    {
        "name": "cloud_get_wifi_transmit_power",
        "description": "Fetch Wi-Fi transmit power by network and band.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id", "band"],
            "properties": {
                "network_id": {"type": "string"},
                "band": {"type": "integer", "enum": [0, 1, 2, 3]},
                "refresh": {"type": "boolean", "default": True},
            },
        },
    },
    {
        "name": "cloud_update_network",
        "description": "Update TAUC cloud network-system-management network object. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id", "payload"],
            "properties": {
                "network_id": {"type": "string"},
                "payload": {"type": "object"},
            },
        },
    },
    {
        "name": "cloud_delete_network",
        "description": "Delete TAUC cloud network-system-management network object. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {"network_id": {"type": "string"}},
        },
    },
    {
        "name": "cloud_reset_device",
        "description": "Reset an Aginet device by device id. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["device_id"],
            "properties": {"device_id": {"type": "string"}},
        },
    },
    {
        "name": "cloud_set_pppoe_credentials",
        "description": "Update PPPoE credentials for a network. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id", "username", "password"],
            "properties": {
                "network_id": {"type": "string"},
                "username": {"type": "string"},
                "password": {"type": "string"},
            },
        },
    },
    {
        "name": "cloud_set_wifi_transmit_power",
        "description": "Update Wi-Fi transmit power payload for a network. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id", "payload"],
            "properties": {
                "network_id": {"type": "string"},
                "payload": {"type": "object"},
            },
        },
    },
    {
        "name": "cloud_block_client",
        "description": "Block a client MAC on a network. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id", "mac"],
            "properties": {
                "network_id": {"type": "string"},
                "mac": {"type": "string"},
            },
        },
    },
    {
        "name": "cloud_unblock_client",
        "description": "Unblock a client MAC on a network. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id", "mac"],
            "properties": {
                "network_id": {"type": "string"},
                "mac": {"type": "string"},
            },
        },
    },
    {
        "name": "acs_get_device_id",
        "description": "Resolve a TAUC ACS deviceId from serial number and MAC address.",
        "inputSchema": {
            "type": "object",
            "required": ["sn", "mac"],
            "properties": {
                "sn": {"type": "string"},
                "mac": {"type": "string"},
            },
        },
    },
    {
        "name": "acs_get_device_detail",
        "description": "Fetch TAUC ACS device detail by deviceId.",
        "inputSchema": {
            "type": "object",
            "required": ["device_id"],
            "properties": {"device_id": {"type": "string"}},
        },
    },
    {
        "name": "acs_get_internet",
        "description": "Fetch WAN/internet state for a TAUC ACS device.",
        "inputSchema": {
            "type": "object",
            "required": ["device_id"],
            "properties": {"device_id": {"type": "string"}},
        },
    },
    {
        "name": "acs_get_wifi",
        "description": "Fetch Wi-Fi list/configuration for a TAUC ACS device.",
        "inputSchema": {
            "type": "object",
            "required": ["device_id"],
            "properties": {"device_id": {"type": "string"}},
        },
    },
    {
        "name": "acs_get_lan_config",
        "description": "Fetch LAN configuration for a TAUC ACS device.",
        "inputSchema": {
            "type": "object",
            "required": ["device_id"],
            "properties": {"device_id": {"type": "string"}},
        },
    },
    {
        "name": "acs_get_dhcp_config",
        "description": "Fetch DHCP server configuration for a TAUC ACS device.",
        "inputSchema": {
            "type": "object",
            "required": ["device_id"],
            "properties": {"device_id": {"type": "string"}},
        },
    },
    {
        "name": "acs_get_tr_tree",
        "description": "Fetch TR-069 tree data for a TAUC ACS device.",
        "inputSchema": {
            "type": "object",
            "required": ["device_id"],
            "properties": {"device_id": {"type": "string"}},
        },
    },
    {
        "name": "acs_get_task_result",
        "description": "Fetch TAUC ACS task execution result by taskId.",
        "inputSchema": {
            "type": "object",
            "required": ["task_id"],
            "properties": {"task_id": {"type": "string"}},
        },
    },
    {
        "name": "acs_reboot_device",
        "description": "Queue a reboot task for a TAUC ACS device. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["device_id"],
            "properties": {"device_id": {"type": "string"}},
        },
    },
    {
        "name": "olt_get_devices",
        "description": "Fetch TAUC OLT devices with optional filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "mac": {"type": "string"},
                "sn": {"type": "string"},
                "status": {"type": "string"},
                "page": {"type": "integer", "default": 0},
                "page_size": {"type": "integer", "default": 50},
            },
        },
    },
    {
        "name": "olt_get_device_ids",
        "description": "Fetch TAUC OLT device ids with optional filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "mac": {"type": "string"},
                "sn": {"type": "string"},
                "status": {"type": "string"},
                "page": {"type": "integer", "default": 0},
                "page_size": {"type": "integer", "default": 50},
            },
        },
    },
    {
        "name": "olt_get_device",
        "description": "Fetch one TAUC OLT device by MAC or ID.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id"],
            "properties": {"mac_or_id": {"type": "string"}},
        },
    },
    {
        "name": "olt_get_device_name",
        "description": "Fetch device name for one TAUC OLT device.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id"],
            "properties": {"mac_or_id": {"type": "string"}},
        },
    },
    {
        "name": "olt_get_pon_ports",
        "description": "Fetch PON port inventory for one TAUC OLT device.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id"],
            "properties": {"mac_or_id": {"type": "string"}},
        },
    },
    {
        "name": "olt_get_onu_devices",
        "description": "Fetch TAUC ONU inventory with optional filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "fuzzy_word": {"type": "string"},
                "sns": {"type": "array", "items": {"type": "string"}},
                "status": {"type": "string"},
                "page": {"type": "integer", "default": 0},
                "page_size": {"type": "integer", "default": 50},
            },
        },
    },
    {
        "name": "olt_get_onu_admin_status",
        "description": "Fetch admin status for a specific ONU under a specific OLT.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id", "onu_id"],
            "properties": {
                "mac_or_id": {"type": "string"},
                "onu_id": {"type": "string"},
            },
        },
    },
    {
        "name": "olt_get_onu_description",
        "description": "Fetch description for a specific ONU under a specific OLT.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id", "onu_id"],
            "properties": {
                "mac_or_id": {"type": "string"},
                "onu_id": {"type": "string"},
            },
        },
    },
    {
        "name": "olt_get_reboot_status",
        "description": "Fetch reboot status for a TAUC OLT device.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id"],
            "properties": {"mac_or_id": {"type": "string"}},
        },
    },
    {
        "name": "olt_reboot_device",
        "description": "Reboot a TAUC OLT device. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id"],
            "properties": {
                "mac_or_id": {"type": "string"},
                "save_current_config": {"type": "boolean", "default": True},
            },
        },
    },
    {
        "name": "olt_reboot_onus",
        "description": "Reboot a batch of ONUs under one OLT. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id", "onu_ids"],
            "properties": {
                "mac_or_id": {"type": "string"},
                "onu_ids": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "olt_get_onu_reboot_status",
        "description": "Fetch reboot status for a batch of ONUs under one OLT.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id", "onu_ids"],
            "properties": {
                "mac_or_id": {"type": "string"},
                "onu_ids": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "olt_set_onu_admin_status",
        "description": "Set ONU admin status ACTIVATE/DEACTIVATE. Disabled unless TAUC_ENABLE_WRITES=true.",
        "inputSchema": {
            "type": "object",
            "required": ["mac_or_id", "onu_id", "admin_status"],
            "properties": {
                "mac_or_id": {"type": "string"},
                "onu_id": {"type": "string"},
                "admin_status": {"type": "string", "enum": ["ACTIVATE", "DEACTIVATE"]},
            },
        },
    },
]


def load_local_env() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def canonical_json_body(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


class TaucConfigError(RuntimeError):
    pass


class TaucClient:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.base_url = os.environ.get(f"TAUC_{prefix}_BASE_URL", "").rstrip("/")
        self.auth_type = os.environ.get(f"TAUC_{prefix}_AUTH_TYPE", os.environ.get("TAUC_AUTH_TYPE", "aksk")).strip().lower()
        self.verify_ssl = as_bool(os.environ.get(f"TAUC_{prefix}_VERIFY_SSL", os.environ.get("TAUC_VERIFY_SSL", "true")), True)
        self.cert_file = os.environ.get(f"TAUC_{prefix}_CLIENT_CERT", os.environ.get("TAUC_CLIENT_CERT", ""))
        self.key_file = os.environ.get(f"TAUC_{prefix}_CLIENT_KEY", os.environ.get("TAUC_CLIENT_KEY", ""))
        self.key_password = os.environ.get(f"TAUC_{prefix}_CLIENT_KEY_PASSWORD", os.environ.get("TAUC_CLIENT_KEY_PASSWORD", ""))
        self.ca_file = os.environ.get(f"TAUC_{prefix}_CA_CERT", os.environ.get("TAUC_CA_CERT", ""))
        self.access_key = os.environ.get(f"TAUC_{prefix}_ACCESS_KEY", os.environ.get("TAUC_ACCESS_KEY", ""))
        self.secret_key = os.environ.get(f"TAUC_{prefix}_SECRET_KEY", os.environ.get("TAUC_SECRET_KEY", ""))
        self.client_id = os.environ.get(f"TAUC_{prefix}_CLIENT_ID", os.environ.get("TAUC_CLIENT_ID", ""))
        self.client_secret = os.environ.get(f"TAUC_{prefix}_CLIENT_SECRET", os.environ.get("TAUC_CLIENT_SECRET", ""))
        self._oauth_token: str | None = None
        self._oauth_expiry: float = 0.0

    def configured(self) -> bool:
        return bool(self.base_url and self.cert_file and self.key_file and self._auth_creds_present())

    def _auth_creds_present(self) -> bool:
        if self.auth_type == "oauth2":
            return bool(self.client_id and self.client_secret)
        return bool(self.access_key and self.secret_key)

    def _ssl_context(self) -> ssl.SSLContext:
        if not self.cert_file or not self.key_file:
            raise TaucConfigError(f"TAUC_{self.prefix}_CLIENT_CERT or TAUC_{self.prefix}_CLIENT_KEY is not configured")
        if self.ca_file and Path(self.ca_file).exists():
            context = ssl.create_default_context(cafile=self.ca_file)
        else:
            context = ssl.create_default_context()
        if not self.verify_ssl:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(certfile=self.cert_file, keyfile=self.key_file, password=self.key_password or None)
        return context

    def _content_md5(self, body: bytes) -> str:
        return base64.b64encode(hashlib.md5(body).digest()).decode("ascii")

    def _sign(self, path: str, body: bytes | None = None) -> tuple[str, str]:
        timestamp = str(int(time.time()))
        nonce = str(uuid.uuid4())
        pieces: list[str] = []
        if body:
            pieces.append(self._content_md5(body))
        pieces.extend([timestamp, nonce, path])
        string_to_sign = "\n".join(pieces)
        secret = self.client_secret if self.auth_type == "oauth2" else self.secret_key
        if not secret:
            raise TaucConfigError(f"TAUC {self.prefix} secret is not configured")
        signature = hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        if self.auth_type == "oauth2":
            x_auth = f"Nonce={nonce},Signature={signature},Timestamp={timestamp}"
        else:
            x_auth = f"Nonce={nonce},AccessKey={self.access_key},Signature={signature},Timestamp={timestamp}"
        return x_auth, timestamp

    def _oauth_access_token(self) -> str:
        if self.auth_type != "oauth2":
            raise TaucConfigError("OAuth token requested while auth_type is not oauth2")
        if self._oauth_token and time.time() < self._oauth_expiry - 60:
            return self._oauth_token
        token_path = "/v1/openapi/token" if self.prefix in {"ACS", "CLOUD"} else "/token"
        body_map = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        body = urllib.parse.urlencode(body_map).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}{token_path}",
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, context=self._ssl_context(), timeout=30) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"TAUC {self.prefix} token HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"TAUC {self.prefix} token connection error: {exc}") from exc
        token = (((payload or {}).get("result") or {}).get("access_token")) or ""
        expires = int((((payload or {}).get("result") or {}).get("expires_in")) or 3600)
        if not token:
            raise RuntimeError(f"TAUC {self.prefix} token response missing access_token: {payload}")
        self._oauth_token = token
        self._oauth_expiry = time.time() + expires
        return token

    def request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | list[Any] | None = None,
    ) -> Any:
        if not self.configured():
            raise TaucConfigError(f"TAUC {self.prefix} is not fully configured")
        query = {k: v for k, v in (query or {}).items() if v is not None}
        query_pairs: list[tuple[str, Any]] = []
        for key, value in query.items():
            if isinstance(value, list):
                if key == "sns":
                    query_pairs.append((key, ",".join(str(item) for item in value)))
                else:
                    for item in value:
                        query_pairs.append((key, item))
            else:
                query_pairs.append((key, value))
        query_string = urllib.parse.urlencode(query_pairs, doseq=True)
        signed_path = path
        if self.prefix != "CLOUD" and query_string:
            signed_path = f"{path}?{query_string}"
        payload = canonical_json_body(body) if body is not None else None
        x_auth, _timestamp = self._sign(signed_path, payload)
        url = f"{self.base_url}{path}"
        if query_string:
            url = f"{url}?{query_string}"
        headers = {"Accept": "application/json", "X-Authorization": x_auth}
        if payload is not None:
            headers["Content-Type"] = "application/json"
            headers["Content-MD5"] = self._content_md5(payload)
        if self.auth_type == "oauth2":
            headers["Authorization"] = f"Bearer {self._oauth_access_token()}"
        req = urllib.request.Request(url, data=payload, method=method.upper(), headers=headers)
        try:
            with urllib.request.urlopen(req, context=self._ssl_context(), timeout=30) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"TAUC {self.prefix} HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"TAUC {self.prefix} connection error: {exc}") from exc
        return json.loads(raw) if raw else {}

    def probe(self) -> dict[str, Any]:
        diag = {
            "configured": self.configured(),
            "base_url": self.base_url,
            "auth_type": self.auth_type,
            "verify_ssl": self.verify_ssl,
            "has_client_cert": bool(self.cert_file),
            "has_client_key": bool(self.key_file),
            "has_ca_cert": bool(self.ca_file),
        }
        if not self.configured():
            return {"ok": False, **diag}
        try:
            if self.prefix == "ACS":
                payload = self.request("GET", "/v1/openapi/wiki/client")
            else:
                payload = self.request("GET", "/wiki/client")
            return {"ok": True, **diag, "probe_path": payload}
        except Exception as exc:
            return {"ok": False, **diag, "error": str(exc)}


class TaucMCPServer:
    def __init__(self) -> None:
        load_local_env()
        self.cloud = TaucClient("CLOUD")
        self.acs = TaucClient("ACS")
        self.olt = TaucClient("OLT")
        self.enable_writes = as_bool(os.environ.get("TAUC_ENABLE_WRITES"), False)

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
                    "serverInfo": {"name": "tauc-mcp", "version": "0.1.0"},
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

    def _ensure_writes_enabled(self) -> None:
        if not self.enable_writes:
            raise PermissionError("TAUC write tools are disabled. Set TAUC_ENABLE_WRITES=true to allow write operations.")

    def _call_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        args = params.get("arguments") or {}
        if name == "get_server_info":
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "writes_enabled": self.enable_writes,
                                "cloud": self.cloud.probe(),
                                "acs": self.acs.probe(),
                                "olt": self.olt.probe(),
                                "tools": [tool["name"] for tool in TOOLS],
                            }
                        ),
                    }
                ]
            }
        if name == "cloud_get_network_name_list":
            path = f"/v1/openapi/network-system-management/network-name-list/{args['status']}"
            data = self.cloud.request("GET", path, query={"page": int(args.get("page", 0)), "pageSize": int(args.get("page_size", 100))})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_get_network_details":
            data = self.cloud.request("GET", f"/v1/openapi/network-system-management/details/{urllib.parse.quote(args['network_id'], safe='')}")
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_get_network":
            data = self.cloud.request("GET", f"/v1/openapi/network-system-management/network/{urllib.parse.quote(args['network_id'], safe='')}")
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_get_preconfiguration_status":
            data = self.cloud.request("GET", f"/v1/openapi/device-management/aginet/preconfiguration-status/{urllib.parse.quote(args['network_id'], safe='')}")
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_get_pppoe_status":
            data = self.cloud.request(
                "GET",
                f"/v1/openapi/device-management/aginet/pppoe-credentials/configured-status/{urllib.parse.quote(args['network_id'], safe='')}",
                query={
                    "refresh": str(bool(args.get("refresh", True))).lower(),
                    "includeCredentials": str(bool(args.get("include_credentials", False))).lower(),
                },
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_get_pppoe_credentials":
            data = self.cloud.request(
                "GET",
                f"/v1/openapi/device-management/aginet/pppoe-credentials/{urllib.parse.quote(args['network_id'], safe='')}",
                query={"refresh": str(bool(args.get("refresh", True))).lower()},
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_get_wifi_transmit_power":
            data = self.cloud.request(
                "GET",
                f"/v1/openapi/device-management/aginet/wifi-transmit-power/{urllib.parse.quote(args['network_id'], safe='')}",
                query={
                    "refresh": str(bool(args.get("refresh", True))).lower(),
                    "band": int(args["band"]),
                },
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_update_network":
            self._ensure_writes_enabled()
            data = self.cloud.request(
                "PUT",
                f"/v1/openapi/network-system-management/network/{urllib.parse.quote(args['network_id'], safe='')}",
                body=args["payload"],
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_delete_network":
            self._ensure_writes_enabled()
            data = self.cloud.request("DELETE", f"/v1/openapi/network-system-management/network/{urllib.parse.quote(args['network_id'], safe='')}")
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_reset_device":
            self._ensure_writes_enabled()
            data = self.cloud.request("POST", f"/v1/openapi/device-management/aginet/reset-all/{urllib.parse.quote(args['device_id'], safe='')}")
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_set_pppoe_credentials":
            self._ensure_writes_enabled()
            data = self.cloud.request(
                "PUT",
                f"/v1/openapi/device-management/aginet/pppoe-credentials/{urllib.parse.quote(args['network_id'], safe='')}",
                body={"username": args["username"], "password": args["password"]},
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_set_wifi_transmit_power":
            self._ensure_writes_enabled()
            data = self.cloud.request(
                "PUT",
                f"/v1/openapi/device-management/aginet/wifi-transmit-power/{urllib.parse.quote(args['network_id'], safe='')}",
                body=args["payload"],
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_block_client":
            self._ensure_writes_enabled()
            data = self.cloud.request(
                "PUT",
                f"/v1/openapi/device-management/aginet/{urllib.parse.quote(args['network_id'], safe='')}/client/tr/block",
                body={"mac": args["mac"]},
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "cloud_unblock_client":
            self._ensure_writes_enabled()
            data = self.cloud.request(
                "PUT",
                f"/v1/openapi/device-management/aginet/{urllib.parse.quote(args['network_id'], safe='')}/client/tr/unblock",
                body={"mac": args["mac"]},
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_get_device_id":
            data = self.acs.request("GET", "/v1/openapi/acs/device/device-id", query={"sn": args["sn"], "mac": args["mac"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_get_device_detail":
            data = self.acs.request("GET", "/v1/openapi/acs/device/detail", query={"deviceId": args["device_id"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_get_internet":
            data = self.acs.request("GET", "/v1/openapi/acs/device/internet", query={"deviceId": args["device_id"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_get_wifi":
            data = self.acs.request("GET", "/v1/openapi/acs/device/wireless/wifi", query={"deviceId": args["device_id"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_get_lan_config":
            data = self.acs.request("GET", "/v1/openapi/acs/device/lan-config", query={"deviceId": args["device_id"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_get_dhcp_config":
            data = self.acs.request("GET", "/v1/openapi/acs/device/dhcp-server/dhcp-server-config", query={"deviceId": args["device_id"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_get_tr_tree":
            data = self.acs.request("GET", "/v1/openapi/acs/device/tr-tree", query={"deviceId": args["device_id"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_get_task_result":
            data = self.acs.request("GET", "/v1/openapi/acs/task/result", query={"taskId": args["task_id"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "acs_reboot_device":
            self._ensure_writes_enabled()
            data = self.acs.request("POST", "/v1/openapi/acs/device/device-system/reboot", query={"deviceId": args["device_id"]}, body={})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_devices":
            data = self.olt.request(
                "GET",
                "/olt/devices",
                query={
                    "mac": args.get("mac"),
                    "sn": args.get("sn"),
                    "status": args.get("status"),
                    "page": int(args.get("page", 0)),
                    "pageSize": int(args.get("page_size", 50)),
                },
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_device_ids":
            data = self.olt.request(
                "GET",
                "/olt/devices/ids",
                query={
                    "mac": args.get("mac"),
                    "sn": args.get("sn"),
                    "status": args.get("status"),
                    "page": int(args.get("page", 0)),
                    "pageSize": int(args.get("page_size", 50)),
                },
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_device":
            data = self.olt.request("GET", f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}")
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_device_name":
            data = self.olt.request("GET", f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/device-name")
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_pon_ports":
            data = self.olt.request("GET", f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/normal/pon-ports")
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_onu_devices":
            data = self.olt.request(
                "GET",
                "/olt/devices/onu-devices",
                query={
                    "fuzzyWord": args.get("fuzzy_word"),
                    "sns": args.get("sns"),
                    "status": args.get("status"),
                    "page": int(args.get("page", 0)),
                    "pageSize": int(args.get("page_size", 50)),
                },
            )
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_onu_admin_status":
            path = f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/onu-devices/{urllib.parse.quote(args['onu_id'], safe='')}/admin-status"
            data = self.olt.request("GET", path)
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_onu_description":
            path = f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/onu-devices/{urllib.parse.quote(args['onu_id'], safe='')}/description"
            data = self.olt.request("GET", path)
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_reboot_status":
            path = f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/reboot/now/status"
            data = self.olt.request("GET", path)
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_reboot_device":
            self._ensure_writes_enabled()
            path = f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/reboot/now"
            body = {}
            if "save_current_config" in args:
                body["saveCurrentConfig"] = bool(args.get("save_current_config", True))
            data = self.olt.request("POST", path, body=body)
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_reboot_onus":
            self._ensure_writes_enabled()
            path = f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/onu-devices/reboot"
            data = self.olt.request("POST", path, body={"onuIds": args["onu_ids"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_get_onu_reboot_status":
            path = f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/onu-devices/reboot/status"
            data = self.olt.request("POST", path, body={"onuIds": args["onu_ids"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        if name == "olt_set_onu_admin_status":
            self._ensure_writes_enabled()
            path = f"/olt/devices/{urllib.parse.quote(args['mac_or_id'], safe='')}/onu-devices/{urllib.parse.quote(args['onu_id'], safe='')}/admin-status"
            data = self.olt.request("PUT", path, body={"adminStatus": args["admin_status"]})
            return {"content": [{"type": "text", "text": json.dumps(data)}]}
        raise ValueError(f"Unknown tool: {name}")

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
            if not line:
                return None
            if line in (b"\r\n", b"\n"):
                break
        body = sys.stdin.buffer.read(content_length)
        if not body:
            return None
        return json.loads(body.decode("utf-8"))

    def _write_message(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message).encode("utf-8")
        sys.stdout.buffer.write(f"Content-Length: {len(payload)}\r\n\r\n".encode("utf-8"))
        sys.stdout.buffer.write(payload)
        sys.stdout.buffer.flush()


if __name__ == "__main__":
    TaucMCPServer().run()
