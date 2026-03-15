#!/usr/bin/env python3
from __future__ import annotations

import os
import urllib.parse
from typing import Any

from packages.jake.connectors.mcp.tauc_mcp import TaucClient
from packages.jake.connectors.mcp.vilo_mcp import ViloClient, getenv_fallback


class TaucOpsAdapter:
    def __init__(self) -> None:
        self._seed_env('CLOUD')
        self._seed_env('ACS')
        self._seed_env('OLT')
        self.cloud = TaucClient('CLOUD')
        self.acs = TaucClient('ACS')
        self.olt = TaucClient('OLT')

    def _seed_env(self, prefix: str) -> None:
        names = [
            f'TAUC_{prefix}_BASE_URL',
            f'TAUC_{prefix}_AUTH_TYPE',
            f'TAUC_{prefix}_ACCESS_KEY',
            f'TAUC_{prefix}_SECRET_KEY',
            f'TAUC_{prefix}_CLIENT_ID',
            f'TAUC_{prefix}_CLIENT_SECRET',
            f'TAUC_{prefix}_CLIENT_CERT',
            f'TAUC_{prefix}_CLIENT_KEY',
            f'TAUC_{prefix}_CLIENT_KEY_PASSWORD',
            f'TAUC_{prefix}_CA_CERT',
            'TAUC_VERIFY_SSL',
        ]
        for name in names:
            value = getenv_fallback(name, 'tauc_mcp')
            if value and not os.environ.get(name):
                os.environ[name] = value

    def summary(self) -> dict[str, Any]:
        return {
            'cloud_configured': self.cloud.configured() if self.cloud else False,
            'acs_configured': self.acs.configured() if self.acs else False,
            'olt_configured': self.olt.configured() if self.olt else False,
        }

    def get_network_name_list(self, status: str, page: int = 0, page_size: int = 100, name_prefix: str | None = None) -> dict[str, Any]:
        if not self.cloud or not self.cloud.configured():
            raise ValueError('TAUC cloud is not configured')
        status = status.upper()
        if status not in {'ONLINE', 'ABNORMAL'}:
            raise ValueError('status must be ONLINE or ABNORMAL')
        payload = self.cloud.request(
            'GET',
            f'/v1/openapi/network-system-management/network-name-list/{status}',
            query={'page': int(page), 'pageSize': int(page_size)},
        )
        results = (((payload or {}).get('result') or {}).get('data')) or []
        if name_prefix:
            needle = str(name_prefix).lower()
            results = [r for r in results if needle in str(r.get('networkName') or '').lower()]
        return {
            'status': status,
            'page': int(page),
            'page_size': int(page_size),
            'name_prefix': name_prefix,
            'count': len(results),
            'results': results,
            'raw': payload,
        }

    def get_network_details(self, network_id: str) -> dict[str, Any]:
        if not self.cloud or not self.cloud.configured():
            raise ValueError('TAUC cloud is not configured')
        return self.cloud.request('GET', f"/v1/openapi/network-system-management/details/{urllib.parse.quote(network_id, safe='')}")

    def get_preconfiguration_status(self, network_id: str) -> dict[str, Any]:
        if not self.cloud or not self.cloud.configured():
            raise ValueError('TAUC cloud is not configured')
        return self.cloud.request('GET', f"/v1/openapi/device-management/aginet/preconfiguration-status/{urllib.parse.quote(network_id, safe='')}")

    def get_pppoe_status(self, network_id: str, refresh: bool = True, include_credentials: bool = False) -> dict[str, Any]:
        if not self.cloud or not self.cloud.configured():
            raise ValueError('TAUC cloud is not configured')
        return self.cloud.request(
            'GET',
            f"/v1/openapi/device-management/aginet/pppoe-credentials/configured-status/{urllib.parse.quote(network_id, safe='')}",
            query={
                'refresh': str(bool(refresh)).lower(),
                'includeCredentials': str(bool(include_credentials)).lower(),
            },
        )

    def get_device_id(self, sn: str, mac: str) -> dict[str, Any]:
        if self.cloud and self.cloud.configured():
            return self.cloud.request('GET', '/v1/openapi/device-information/device-id', query={'sn': sn, 'mac': mac})
        if self.acs and self.acs.configured():
            return self.acs.request('GET', '/v1/openapi/acs/device/device-id', query={'sn': sn, 'mac': mac})
        raise ValueError('TAUC cloud or ACS is not configured')

    def get_device_detail(self, device_id: str) -> dict[str, Any]:
        if self.cloud and self.cloud.configured():
            return self.cloud.request('GET', f"/v1/openapi/device-information/device-info/{urllib.parse.quote(device_id, safe='')}")
        if self.acs and self.acs.configured():
            return self.acs.request('GET', '/v1/openapi/acs/device/detail', query={'deviceId': device_id})
        raise ValueError('TAUC cloud or ACS is not configured')

    def get_device_internet(self, device_id: str) -> dict[str, Any]:
        if not self.acs or not self.acs.configured():
            raise ValueError('TAUC ACS is not configured')
        return self.acs.request('GET', '/v1/openapi/acs/device/internet', query={'deviceId': device_id})

    def get_olt_devices(self, mac: str | None, sn: str | None, status: str | None, page: int = 0, page_size: int = 50) -> dict[str, Any]:
        if not self.olt or not self.olt.configured():
            raise ValueError('TAUC OLT is not configured')
        return self.olt.request('GET', '/olt/devices', query={'mac': mac, 'sn': sn, 'status': status, 'page': int(page), 'pageSize': int(page_size)})


class ViloOpsAdapter:
    def __init__(self) -> None:
        self.client = ViloClient()

    def configured(self) -> bool:
        return bool(self.client and self.client.configured())

    def summary(self) -> dict[str, Any]:
        return self.client.diagnostics() if self.client else {'configured': False}

    def get_inventory(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.configured():
            raise ValueError('Vilo API is not configured')
        return self.client.get_inventory(page_index, page_size)

    def search_inventory(self, filter_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.configured():
            raise ValueError('Vilo API is not configured')
        return self.client.search_inventory(filter_group or [], page_index, page_size)

    def get_subscribers(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.configured():
            raise ValueError('Vilo API is not configured')
        return self.client.get_subscribers(page_index, page_size)

    def search_subscribers(self, filter_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.configured():
            raise ValueError('Vilo API is not configured')
        return self.client.search_subscribers(filter_group or [], page_index, page_size)

    def get_networks(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.configured():
            raise ValueError('Vilo API is not configured')
        return self.client.get_networks(page_index, page_size)

    def search_networks(self, filter_group: list[dict[str, Any]] | None = None, sort_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.configured():
            raise ValueError('Vilo API is not configured')
        return self.client.search_networks(filter_group or [], sort_group or [], page_index, page_size)

    def get_devices(self, network_id: str) -> dict[str, Any]:
        if not self.configured():
            raise ValueError('Vilo API is not configured')
        return self.client.get_vilos(network_id)

    def search_devices(self, network_id: str, sort_group: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        if not self.configured():
            raise ValueError('Vilo API is not configured')
        return self.client.search_vilos(network_id, sort_group or [])
