#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

TOOLS = [
    {
        "name": "get_server_info",
        "description": "Return Vilo MCP configuration status, token cache status, and API base URL.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_inventory",
        "description": "Fetch Vilo inventory page.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
            },
        },
    },
    {
        "name": "search_inventory",
        "description": "Search Vilo inventory by supported filter keys such as status, device_mac, device_sn, subscriber_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
                "filter": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["key", "value"],
                        "properties": {
                            "key": {"type": "string"},
                            "value": {},
                        },
                    },
                    "default": [],
                },
            },
        },
    },
    {
        "name": "get_subscribers",
        "description": "Fetch Vilo subscribers page.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
            },
        },
    },
    {
        "name": "search_subscribers",
        "description": "Search Vilo subscribers by subscriber_id, first_name, last_name, email, or phone.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
                "filter": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["key", "value"],
                        "properties": {
                            "key": {"type": "string"},
                            "value": {},
                        },
                    },
                    "default": [],
                },
            },
        },
    },
    {
        "name": "get_networks",
        "description": "Fetch Vilo networks page.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
            },
        },
    },
    {
        "name": "search_networks",
        "description": "Search Vilo networks by network_id, subscriber_id, user_email, main_vilo_mac, or network_name.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
                "filter": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["key", "value"],
                        "properties": {
                            "key": {"type": "string"},
                            "value": {},
                        },
                    },
                    "default": [],
                },
                "sort": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["key", "type"],
                        "properties": {
                            "key": {"type": "string"},
                            "type": {"type": "integer"},
                        },
                    },
                    "default": [],
                },
            },
        },
    },
    {
        "name": "get_vilos",
        "description": "Fetch Vilo device details for a network_id.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {
                "network_id": {"type": "string"},
            },
        },
    },
    {
        "name": "search_vilos",
        "description": "Search Vilo devices for a network_id with optional sort_group.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {
                "network_id": {"type": "string"},
                "sort_group": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["key", "type"],
                        "properties": {
                            "key": {"type": "string"},
                            "type": {"type": "integer"},
                        },
                    },
                    "default": [],
                },
            },
        },
    },
]


def load_local_env_file() -> None:
    env_path = Path(os.environ.get("JAKE_ENV_FILE", str(Path(__file__).resolve().parents[4] / ".env")))
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_anythingllm_mcp_env(server_name: str) -> dict[str, str]:
    path = Path(os.environ.get("ANYTHINGLLM_MCP_SERVERS_JSON", str(Path.home() / "Library" / "Application Support" / "anythingllm-desktop" / "storage" / "plugins" / "anythingllm_mcp_servers.json")))
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return (data.get('mcpServers', {}).get(server_name, {}) or {}).get('env', {}) or {}
    except Exception:
        return {}


def getenv_fallback(name: str, server_name: str) -> str:
    return os.environ.get(name, '') or load_anythingllm_mcp_env(server_name).get(name, '') or ''


def now_ms() -> int:
    return int(time.time() * 1000)


def md5_hex(value: str) -> str:
    return hashlib.md5(value.encode('utf-8')).hexdigest()


def triple_md5_hex(value: str) -> str:
    out = value
    for _ in range(3):
        out = md5_hex(out)
    return out


def compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


class ViloAPIError(RuntimeError):
    pass


class ViloClient:
    def __init__(self) -> None:
        load_local_env_file()
        self.base_url = (getenv_fallback('VILO_BASE_URL', 'vilo_mcp') or 'https://beta-api.viloliving.com').rstrip('/')
        self.app_key = getenv_fallback('VILO_APPKEY', 'vilo_mcp')
        self.app_secret = getenv_fallback('VILO_APPSECRET', 'vilo_mcp')
        self.timeout = int(getenv_fallback('VILO_TIMEOUT', 'vilo_mcp') or '30')
        self.access_token = ''
        self.refresh_token = ''
        self.token_expires_at = 0
        self.last_token_response: dict[str, Any] | None = None

    def configured(self) -> bool:
        return bool(self.base_url and self.app_key and self.app_secret)

    def diagnostics(self) -> dict[str, Any]:
        return {
            'configured': self.configured(),
            'base_url': self.base_url,
            'has_app_key': bool(self.app_key),
            'has_app_secret': bool(self.app_secret),
            'has_access_token': bool(self.access_token),
            'has_refresh_token': bool(self.refresh_token),
            'token_expires_at_ms': self.token_expires_at or None,
        }

    def _url(self, path: str, query: dict[str, Any] | None = None) -> str:
        query = {k: v for k, v in (query or {}).items() if v is not None}
        encoded = urllib.parse.urlencode(query, doseq=True)
        return f"{self.base_url}{path}" + (f"?{encoded}" if encoded else "")

    def _read_json(self, req: urllib.request.Request) -> dict[str, Any]:
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode('utf-8')
        except urllib.error.HTTPError as exc:
            body = exc.read().decode('utf-8', errors='replace')
            raise ViloAPIError(f"HTTP {exc.code} for {req.full_url}: {body}") from exc
        except urllib.error.URLError as exc:
            raise ViloAPIError(f"Request failed for {req.full_url}: {exc}") from exc
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ViloAPIError(f"Non-JSON response from {req.full_url}: {raw[:500]}") from exc
        code = str(data.get('code', ''))
        if code and code != '1':
            raise ViloAPIError(f"Vilo API error code={code} message={data.get('message', '')}")
        return data

    def _token_request(self) -> dict[str, Any]:
        if not self.configured():
            raise ViloAPIError('Vilo API is not configured')
        timestamp = now_ms()
        enc_appsecret = triple_md5_hex(f"{self.app_secret}{timestamp}")
        url = self._url('/isp/v1/access_token', {
            'appkey': self.app_key,
            'enc_appsecret': enc_appsecret,
            'timestamp': timestamp,
        })
        req = urllib.request.Request(url, headers={'Accept': 'application/json'})
        data = self._read_json(req)
        tokens = data.get('data') or {}
        self.access_token = str(tokens.get('access_token') or '')
        self.refresh_token = str(tokens.get('refresh_token') or '')
        expires_in = int(tokens.get('expires_in') or 0)
        self.token_expires_at = now_ms() + max(expires_in - 60, 0) * 1000
        self.last_token_response = data
        return data

    def ensure_token(self) -> str:
        if not self.configured():
            raise ViloAPIError('Vilo API is not configured')
        if self.access_token and now_ms() < self.token_expires_at:
            return self.access_token
        self._token_request()
        if not self.access_token:
            raise ViloAPIError('Failed to acquire Vilo access token')
        return self.access_token

    def refresh_access_token(self) -> dict[str, Any]:
        if not self.refresh_token:
            return self._token_request()
        path = '/isp/v1/refresh'
        timestamp = now_ms()
        body = {
            'timestamp': timestamp,
            'data': {'refresh_token': self.refresh_token},
        }
        req = urllib.request.Request(
            self._url(path),
            data=compact_json(body).encode('utf-8'),
            headers=self._signed_headers(path, timestamp, body),
            method='POST',
        )
        data = self._read_json(req)
        tokens = data.get('data') or {}
        self.access_token = str(tokens.get('access_token') or self.access_token)
        self.refresh_token = str(tokens.get('refresh_token') or self.refresh_token)
        expires_in = int(tokens.get('expires_in') or 0)
        self.token_expires_at = now_ms() + max(expires_in - 60, 0) * 1000
        self.last_token_response = data
        return data

    def _signature(self, timestamp: int, body: dict[str, Any] | None) -> str:
        payload = compact_json(body or {})
        msg = f"{self.app_key}{timestamp}{payload}"
        return hmac.new(self.app_secret.encode('utf-8'), msg.encode('utf-8'), hashlib.sha256).hexdigest()

    def _signed_headers(self, path: str, timestamp: int, body: dict[str, Any] | None) -> dict[str, str]:
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'H-AppKey': self.app_key,
            'H-AccessToken': self.ensure_token(),
            'H-Signature': self._signature(timestamp, body),
            'H-SignatureMethod': '1',
            'H-AccessPath': path,
            'Cache-Control': 'no-cache',
        }
        return headers

    def _get(self, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
        timestamp = now_ms()
        query = dict(query or {})
        query['timestamp'] = timestamp
        req = urllib.request.Request(
            self._url(path, query),
            headers=self._signed_headers(path, timestamp, {}),
        )
        return self._read_json(req)

    def _post(self, path: str, data_obj: dict[str, Any]) -> dict[str, Any]:
        timestamp = now_ms()
        body = {'timestamp': timestamp, 'data': data_obj}
        req = urllib.request.Request(
            self._url(path),
            data=compact_json(body).encode('utf-8'),
            headers=self._signed_headers(path, timestamp, body),
            method='POST',
        )
        return self._read_json(req)

    def get_inventory(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        return self._get('/isp/v1/inventory', {'page_index': int(page_index), 'page_size': int(page_size)})

    def search_inventory(self, filter_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        return self._post('/isp/v1/inventory', {
            'page_index': int(page_index),
            'page_size': int(page_size),
            'filter': filter_group or [],
        })

    def get_subscribers(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        return self._get('/isp/v1/subscribers', {'page_index': int(page_index), 'page_size': int(page_size)})

    def search_subscribers(self, filter_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        return self._post('/isp/v1/subscribers', {
            'page_index': int(page_index),
            'page_size': int(page_size),
            'filter': filter_group or [],
        })

    def get_networks(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        return self._get('/isp/v1/networks', {'page_index': int(page_index), 'page_size': int(page_size)})

    def search_networks(self, filter_group: list[dict[str, Any]] | None = None, sort_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        payload = {
            'page_index': int(page_index),
            'page_size': int(page_size),
            'filter': filter_group or [],
        }
        if sort_group:
            payload['sort'] = sort_group
        return self._post('/isp/v1/networks', payload)

    def get_vilos(self, network_id: str) -> dict[str, Any]:
        return self._get('/isp/v1/vilos', {'network_id': str(network_id)})

    def search_vilos(self, network_id: str, sort_group: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {'network_id': str(network_id)}
        if sort_group:
            payload['sort_group'] = sort_group
        return self._post('/isp/v1/vilos', payload)


class ViloMCPServer:
    def __init__(self) -> None:
        self.client = ViloClient()

    def run(self) -> None:
        while True:
            message = self._read_message()
            if message is None:
                return
            if 'method' in message and message.get('id') is None:
                continue
            self._handle_request(message)

    def _handle_request(self, message: dict[str, Any]) -> None:
        request_id = message.get('id')
        method = message.get('method')
        try:
            if method == 'initialize':
                result = {
                    'protocolVersion': '2024-11-05',
                    'capabilities': {'tools': {'listChanged': False}},
                    'serverInfo': {'name': 'vilo-mcp', 'version': '0.1.0'},
                }
            elif method == 'ping':
                result = {}
            elif method == 'tools/list':
                result = {'tools': TOOLS}
            elif method == 'tools/call':
                result = self._call_tool(message.get('params', {}))
            else:
                raise ValueError(f'Unsupported method: {method}')
            self._write_message({'jsonrpc': '2.0', 'id': request_id, 'result': result})
        except Exception as exc:
            self._write_message({'jsonrpc': '2.0', 'id': request_id, 'error': {'code': -32000, 'message': str(exc), 'data': traceback.format_exc()}})

    def _call_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get('name')
        arguments = params.get('arguments') or {}
        if name == 'get_server_info':
            data = self.client.diagnostics()
        elif name == 'get_inventory':
            data = self.client.get_inventory(int(arguments.get('page_index', 1)), int(arguments.get('page_size', 20)))
        elif name == 'search_inventory':
            data = self.client.search_inventory(arguments.get('filter') or [], int(arguments.get('page_index', 1)), int(arguments.get('page_size', 20)))
        elif name == 'get_subscribers':
            data = self.client.get_subscribers(int(arguments.get('page_index', 1)), int(arguments.get('page_size', 20)))
        elif name == 'search_subscribers':
            data = self.client.search_subscribers(arguments.get('filter') or [], int(arguments.get('page_index', 1)), int(arguments.get('page_size', 20)))
        elif name == 'get_networks':
            data = self.client.get_networks(int(arguments.get('page_index', 1)), int(arguments.get('page_size', 20)))
        elif name == 'search_networks':
            data = self.client.search_networks(arguments.get('filter') or [], arguments.get('sort') or [], int(arguments.get('page_index', 1)), int(arguments.get('page_size', 20)))
        elif name == 'get_vilos':
            data = self.client.get_vilos(arguments['network_id'])
        elif name == 'search_vilos':
            data = self.client.search_vilos(arguments['network_id'], arguments.get('sort_group') or [])
        else:
            raise ValueError(f'Unknown tool: {name}')
        return {'content': [{'type': 'text', 'text': json.dumps(data)}]}

    def _read_message(self) -> dict[str, Any] | None:
        try:
            line = input()
        except EOFError:
            return None
        if not line:
            return None
        return json.loads(line)

    def _write_message(self, message: dict[str, Any]) -> None:
        print(json.dumps(message), flush=True)
