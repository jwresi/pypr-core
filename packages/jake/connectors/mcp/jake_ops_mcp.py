#!/usr/bin/env python3
from __future__ import annotations

import base64
import csv
import json
import os
import re
import sqlite3
import threading
import traceback
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from packages.jake.connectors.vendor_adapters import TaucOpsAdapter, ViloOpsAdapter

TOOLS = [
    {"name": "get_server_info", "description": "Return Jake Ops MCP status and latest scan diagnostics.", "inputSchema": {"type": "object", "properties": {}}},
    {
        "name": "query_summary",
        "description": "Accept a natural-language network operations question and return the deterministic Jake answer with matched action, summary, and raw result.",
        "inputSchema": {"type": "object", "required": ["query"], "properties": {"query": {"type": "string"}}},
    },
    {
        "name": "get_outage_context",
        "description": "Return deterministic outage context for an address/unit report by resolving building scope, checking PPP evidence, related bridge sightings, and active alerts.",
        "inputSchema": {
            "type": "object",
            "required": ["address_text", "unit"],
            "properties": {
                "address_text": {"type": "string"},
                "unit": {"type": "string"},
            },
        },
    },
    {
        "name": "audit_device_labels",
        "description": "Audit network-scan and NetBox device names against the required label format <6 digit location>.<3 digit site>.<device type><2 digit number>.",
        "inputSchema": {"type": "object", "properties": {"include_valid": {"type": "boolean", "default": False}, "limit": {"type": "integer", "default": 500}}},
    },
    {
        "name": "get_subnet_health",
        "description": "Return deterministic health summary for a subnet or site using latest scan, alerts, and cached topology context.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "subnet": {"type": "string"},
                "site_id": {"type": "string"},
                "include_alerts": {"type": "boolean", "default": True},
                "include_bigmac": {"type": "boolean", "default": True},
            },
        },
    },
    {
        "name": "get_online_customers",
        "description": "Return customer online count using latest PPP evidence from the local network map.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": {"type": "string"},
                "site_id": {"type": "string"},
                "building_id": {"type": "string"},
                "router_identity": {"type": "string"},
            },
        },
    },
    {
        "name": "trace_mac",
        "description": "Trace a MAC through the latest scan and optionally corroborate with Bigmac.",
        "inputSchema": {"type": "object", "required": ["mac"], "properties": {"mac": {"type": "string"}, "include_bigmac": {"type": "boolean", "default": True}}},
    },
    {
        "name": "get_netbox_device",
        "description": "Return deterministic NetBox device lookup by exact name.",
        "inputSchema": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}},
    },
    {
        "name": "get_site_alerts",
        "description": "Return active alerts for a site from Alertmanager.",
        "inputSchema": {"type": "object", "required": ["site_id"], "properties": {"site_id": {"type": "string"}}},
    },
    {
        "name": "get_site_summary",
        "description": "Return deterministic site summary using latest scan data, PPP counts, outliers, and optional alerts.",
        "inputSchema": {"type": "object", "required": ["site_id"], "properties": {"site_id": {"type": "string"}, "include_alerts": {"type": "boolean", "default": True}}},
    },
    {
        "name": "get_site_topology",
        "description": "Return deterministic site topology including radios, radio links, resolved addresses, and known unit lists grouped by address.",
        "inputSchema": {"type": "object", "required": ["site_id"], "properties": {"site_id": {"type": "string"}}},
    },
    {
        "name": "get_tauc_network_name_list",
        "description": "Return TAUC cloud network names by status with optional prefix filtering.",
        "inputSchema": {
            "type": "object",
            "required": ["status"],
            "properties": {
                "status": {"type": "string", "enum": ["ONLINE", "ABNORMAL"]},
                "page": {"type": "integer", "default": 0},
                "page_size": {"type": "integer", "default": 100},
                "name_prefix": {"type": "string"},
            },
        },
    },
    {
        "name": "get_tauc_network_details",
        "description": "Return TAUC cloud network details for a network id.",
        "inputSchema": {"type": "object", "required": ["network_id"], "properties": {"network_id": {"type": "string"}}},
    },
    {
        "name": "get_tauc_preconfiguration_status",
        "description": "Return TAUC Aginet preconfiguration status for a network id.",
        "inputSchema": {"type": "object", "required": ["network_id"], "properties": {"network_id": {"type": "string"}}},
    },
    {
        "name": "get_tauc_pppoe_status",
        "description": "Return TAUC Aginet PPPoE configured status for a network id.",
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
        "name": "get_tauc_device_id",
        "description": "Resolve TAUC cloud device id from serial number and MAC address. Falls back to ACS only if cloud is unavailable.",
        "inputSchema": {
            "type": "object",
            "required": ["sn", "mac"],
            "properties": {"sn": {"type": "string"}, "mac": {"type": "string"}},
        },
    },
    {
        "name": "get_tauc_device_detail",
        "description": "Return TAUC cloud device detail by device id. Falls back to ACS only if cloud is unavailable.",
        "inputSchema": {"type": "object", "required": ["device_id"], "properties": {"device_id": {"type": "string"}}},
    },
    {
        "name": "get_tauc_device_internet",
        "description": "Return TAUC ACS WAN/internet state by device id.",
        "inputSchema": {"type": "object", "required": ["device_id"], "properties": {"device_id": {"type": "string"}}},
    },
    {
        "name": "get_tauc_olt_devices",
        "description": "Return TAUC OLT devices with optional filters.",
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
        "name": "get_vilo_server_info",
        "description": "Return Vilo API configuration and token cache status as seen by Jake.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_vilo_inventory",
        "description": "Return Vilo inventory page.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
            },
        },
    },
    {
        "name": "get_vilo_inventory_audit",
        "description": "Reconcile Vilo inventory against the latest scan and customer port map, optionally scoped to one site or building.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "site_id": {"type": "string"},
                "building_id": {"type": "string"},
                "limit": {"type": "integer", "default": 500},
            },
        },
    },
    {
        "name": "export_vilo_inventory_audit",
        "description": "Write Vilo audit JSON, CSV, and Markdown artifacts under output/vilo_audit for one site, one building, or global scope.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "site_id": {"type": "string"},
                "building_id": {"type": "string"},
                "limit": {"type": "integer", "default": 500},
            },
        },
    },
    {
        "name": "search_vilo_inventory",
        "description": "Search Vilo inventory by supported filter keys such as status, device_mac, device_sn, or subscriber_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
                "filter": {"type": "array", "items": {"type": "object"}, "default": []},
            },
        },
    },
    {
        "name": "get_vilo_subscribers",
        "description": "Return Vilo subscriber page.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
            },
        },
    },
    {
        "name": "search_vilo_subscribers",
        "description": "Search Vilo subscribers by subscriber_id, first_name, last_name, email, or phone.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
                "filter": {"type": "array", "items": {"type": "object"}, "default": []},
            },
        },
    },
    {
        "name": "get_vilo_networks",
        "description": "Return Vilo networks page.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
            },
        },
    },
    {
        "name": "search_vilo_networks",
        "description": "Search Vilo networks by network_id, subscriber_id, user_email, main_vilo_mac, or network_name.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_index": {"type": "integer", "default": 1},
                "page_size": {"type": "integer", "default": 20},
                "filter": {"type": "array", "items": {"type": "object"}, "default": []},
                "sort": {"type": "array", "items": {"type": "object"}, "default": []},
            },
        },
    },
    {
        "name": "get_vilo_devices",
        "description": "Return Vilo device details for one network_id.",
        "inputSchema": {"type": "object", "required": ["network_id"], "properties": {"network_id": {"type": "string"}}},
    },
    {
        "name": "search_vilo_devices",
        "description": "Search Vilo devices for one network_id with optional sort_group.",
        "inputSchema": {
            "type": "object",
            "required": ["network_id"],
            "properties": {
                "network_id": {"type": "string"},
                "sort_group": {"type": "array", "items": {"type": "object"}, "default": []},
            },
        },
    },
    {
        "name": "get_building_health",
        "description": "Return deterministic building/switch-block summary for identities such as 000007.055.",
        "inputSchema": {"type": "object", "required": ["building_id"], "properties": {"building_id": {"type": "string"}, "include_alerts": {"type": "boolean", "default": True}}},
    },
    {
        "name": "get_building_model",
        "description": "Return deterministic building model evidence including unit inventory, exact unit-port matches, switches, and direct neighbor edges for a building such as 000007.058.",
        "inputSchema": {"type": "object", "required": ["building_id"], "properties": {"building_id": {"type": "string"}}},
    },
    {
        "name": "get_switch_summary",
        "description": "Return deterministic switch summary for an exact switch identity such as 000007.055.SW04.",
        "inputSchema": {"type": "object", "required": ["switch_identity"], "properties": {"switch_identity": {"type": "string"}}},
    },
    {
        "name": "get_building_customer_count",
        "description": "Return deterministic customer count for a building scope such as 000007.055 across all switches in that building block.",
        "inputSchema": {"type": "object", "required": ["building_id"], "properties": {"building_id": {"type": "string"}}},
    },
    {
        "name": "get_building_flap_history",
        "description": "Return attention ports with flap history for a building scope such as 000007.055 from the customer port map artifact.",
        "inputSchema": {"type": "object", "required": ["building_id"], "properties": {"building_id": {"type": "string"}}},
    },
    {
        "name": "get_site_flap_history",
        "description": "Return attention ports with flap history for an entire site scope such as 000007 from the customer port map artifact.",
        "inputSchema": {"type": "object", "required": ["site_id"], "properties": {"site_id": {"type": "string"}}},
    },
    {
        "name": "get_rogue_dhcp_suspects",
        "description": "Return isolated or suspected rogue DHCP ports for a building or site scope from the customer port map artifact.",
        "inputSchema": {"type": "object", "properties": {"building_id": {"type": "string"}, "site_id": {"type": "string"}}},
    },
    {
        "name": "get_site_rogue_dhcp_summary",
        "description": "Return a deterministic site-wide summary of rogue DHCP ports grouped by building and status from the customer port map artifact.",
        "inputSchema": {"type": "object", "required": ["site_id"], "properties": {"site_id": {"type": "string"}}},
    },
    {
        "name": "get_recovery_ready_cpes",
        "description": "Return recovery-ready or recovery-hold CPE ports for a building or site scope from the customer port map artifact.",
        "inputSchema": {"type": "object", "properties": {"building_id": {"type": "string"}, "site_id": {"type": "string"}}},
    },
    {
        "name": "get_site_punch_list",
        "description": "Return a deterministic site-wide operational punch list from the customer port map artifact, grouped by action class.",
        "inputSchema": {"type": "object", "required": ["site_id"], "properties": {"site_id": {"type": "string"}}},
    },
    {
        "name": "find_cpe_candidates",
        "description": "List probable CPEs from the latest bridge-host view with optional OUI/site/building filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "site_id": {"type": "string"},
                "building_id": {"type": "string"},
                "oui": {"type": "string"},
                "access_only": {"type": "boolean", "default": True},
                "limit": {"type": "integer", "default": 100},
            },
        },
    },
    {
        "name": "get_cpe_state",
        "description": "Return deterministic latest-scan state for a CPE MAC, including bridge, PPP, and ARP correlations.",
        "inputSchema": {"type": "object", "required": ["mac"], "properties": {"mac": {"type": "string"}, "include_bigmac": {"type": "boolean", "default": True}}},
    },
]


def norm_mac(value: str) -> str:
    clean = "".join(ch for ch in value.lower() if ch in "0123456789abcdef")
    if len(clean) != 12:
        return value.lower()
    return ":".join(clean[i : i + 2] for i in range(0, 12, 2))


def mac_vendor_group(mac: str | None) -> str:
    m = norm_mac(mac or "")
    if m.startswith("e8:da:00:"):
        return "vilo"
    if m.startswith(("30:68:93:", "60:83:e7:", "7c:f1:7e:", "d8:44:89:", "dc:62:79:", "e4:fa:c4:")):
        return "tplink"
    return "unknown"


def is_edge_port(interface: str | None) -> bool:
    return bool(interface) and str(interface).startswith("ether")


def is_uplink_like_port(interface: str | None) -> bool:
    if not interface:
        return False
    iface = str(interface).lower()
    return iface.startswith(("sfp", "qsfp", "combo", "bond", "bridge", "vlan"))


def is_direct_physical_interface(interface: str | None) -> bool:
    primary = str(interface or "").split(",", 1)[0].strip().lower()
    return primary.startswith(("ether", "sfp", "qsfp", "combo"))


def is_probable_customer_bridge_host(row: dict[str, Any]) -> bool:
    interface = str(row.get("on_interface") or "")
    if not is_edge_port(interface):
        return False
    if bool(row.get("local")):
        return False
    if bool(row.get("external")):
        return True
    return mac_vendor_group(row.get("mac")) in {"tplink", "vilo"}


def normalize_scope_segment(segment: str) -> str:
    seg = str(segment).strip()
    return str(int(seg)) if seg.isdigit() else seg.upper()


def identity_matches_scope(identity: str | None, scope: str | None) -> bool:
    if not identity or not scope:
        return False
    ident_parts = [normalize_scope_segment(p) for p in str(identity).split(".") if p]
    scope_parts = [normalize_scope_segment(p) for p in str(scope).split(".") if p]
    if len(ident_parts) < len(scope_parts):
        return False
    return ident_parts[: len(scope_parts)] == scope_parts


def canonical_scope(value: str | None) -> str | None:
    if not value:
        return value
    parts = str(value).split(".")
    out: list[str] = []
    for idx, part in enumerate(parts):
        if part.isdigit():
            width = 6 if idx == 0 else 3
            out.append(part.zfill(width))
        else:
            out.append(part.upper())
    return ".".join(out)


def canonical_identity(identity: str | None) -> str | None:
    return canonical_scope(identity)


def normalize_free_text(value: str | None) -> str:
    if not value:
        return ""
    value = value.lower().replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def compact_free_text(value: str | None) -> str:
    return normalize_free_text(value).replace(" ", "")


def parse_unit_token(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).upper()
    m = re.search(r'(\d+[A-Z])\s*$', text)
    if m:
        return m.group(1)
    return None


def parse_unit_parts(value: str | None) -> tuple[int | None, str | None]:
    token = parse_unit_token(value)
    if not token:
        return None, None
    m = re.match(r"(\d+)([A-Z])$", token)
    if not m:
        return None, None
    return int(m.group(1)), m.group(2)


def best_bridge_hit(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None

    def sort_key(row: dict[str, Any]) -> tuple[int, int, int, str]:
        iface = row.get("on_interface")
        return (
            0 if is_edge_port(iface) else 1,
            0 if bool(row.get("external")) else 1,
            0 if bool(row.get("local")) else 1,
            str(iface or ""),
        )

    return sorted(rows, key=sort_key)[0]


def infer_unit_port_candidates(
    target_unit_token: str | None,
    target_floor: int | None,
    target_letter: str | None,
    neighboring_unit_port_hints: list[dict[str, Any]],
    unit_comment_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    # Highest-confidence path: exact unit comment on a customer-facing port.
    for row in unit_comment_rows:
        iface = str(row.get("port") or row.get("interface") or "")
        identity = canonical_identity(row.get("switch_identity") or row.get("device_name") or row.get("identity"))
        if identity and is_edge_port(iface):
            candidates.append(
                {
                    "unit_token": target_unit_token,
                    "identity": identity,
                    "on_interface": iface,
                    "confidence": "high",
                    "reason": f"Exact unit comment match on {identity} {iface}.",
                    "evidence": [row],
                }
            )

    if candidates or target_floor is None or not target_letter:
        return candidates

    floor_rows = []
    for row in neighboring_unit_port_hints:
        unit_token = row.get("unit_token")
        floor, letter = parse_unit_parts(unit_token)
        best_hit = row.get("best_bridge_hit") or {}
        iface = str(best_hit.get("on_interface") or "")
        port_match = re.fullmatch(r"ether(\d+)", iface)
        identity = canonical_identity(best_hit.get("identity"))
        if floor == target_floor and letter and port_match and identity:
            floor_rows.append(
                {
                    "unit_token": unit_token,
                    "floor": floor,
                    "letter": letter,
                    "identity": identity,
                    "on_interface": iface,
                    "ether_number": int(port_match.group(1)),
                    "source_name": row.get("name"),
                }
            )

    floor_rows.sort(key=lambda r: (r["identity"], r["ether_number"], r["letter"]))
    target_ord = ord(target_letter)
    by_identity: dict[str, list[dict[str, Any]]] = {}
    for row in floor_rows:
        by_identity.setdefault(str(row["identity"]), []).append(row)

    for identity, rows in by_identity.items():
        # Best case: infer from two known same-floor units with linear port progression.
        for i, left in enumerate(rows):
            for right in rows[i + 1 :]:
                left_ord = ord(left["letter"])
                right_ord = ord(right["letter"])
                port_delta = right["ether_number"] - left["ether_number"]
                letter_delta = right_ord - left_ord
                if letter_delta <= 0 or port_delta != letter_delta:
                    continue
                if left_ord <= target_ord <= right_ord:
                    inferred_port = left["ether_number"] + (target_ord - left_ord)
                    candidates.append(
                        {
                            "unit_token": target_unit_token,
                            "identity": identity,
                            "on_interface": f"ether{inferred_port}",
                            "confidence": "medium",
                            "reason": f"Same-floor units {left['unit_token']} and {right['unit_token']} map linearly on {identity}; inferred placement for {target_unit_token}.",
                            "evidence": [left, right],
                        }
                    )
                    break
            if candidates:
                break
        if candidates:
            break

    if candidates:
        return candidates

    # Fallback: one-sided adjacent guess if only a single nearby same-floor unit is known.
    nearest: dict[str, Any] | None = None
    nearest_distance: int | None = None
    for rows in by_identity.values():
        for row in rows:
            distance = abs(ord(row["letter"]) - target_ord)
            if nearest is None or distance < (nearest_distance or 999):
                nearest = row
                nearest_distance = distance
    if nearest and nearest_distance is not None and 0 < nearest_distance <= 2:
        offset = target_ord - ord(nearest["letter"])
        inferred_port = nearest["ether_number"] + offset
        if inferred_port > 0:
            candidates.append(
                {
                    "unit_token": target_unit_token,
                    "identity": nearest["identity"],
                    "on_interface": f"ether{inferred_port}",
                    "confidence": "low",
                    "reason": f"Nearest same-floor unit {nearest['unit_token']} is on {nearest['identity']} {nearest['on_interface']}; inferred adjacent placement for {target_unit_token}.",
                    "evidence": [nearest],
                }
            )

    return candidates


REPO_ROOT = Path(__file__).resolve().parents[4]
ARTIFACT_PORT_MAP = Path(os.environ.get("JAKE_PORT_MAP") or os.environ.get("JAKE_ARTIFACT_PORT_MAP") or str(REPO_ROOT / "artifacts" / "customer_port_map" / "customer_port_map.json"))
ARTIFACT_TRANSPORT_RADIO_SCAN = Path(os.environ.get("JAKE_TRANSPORT_RADIO_SCAN") or os.environ.get("JAKE_ARTIFACT_TRANSPORT_RADIO_SCAN") or str(REPO_ROOT / "artifacts" / "transport_radio_scan" / "transport_radio_scan.json"))
NETBOX_RENAME_PROPOSALS_CSV = Path(
    os.environ.get("JAKE_NETBOX_RENAME_PROPOSALS")
    or str(REPO_ROOT / "output" / "spreadsheet" / "netbox_targeted_rename_proposals.csv")
)
VILO_AUDIT_OUT_DIR = Path(os.environ.get("JAKE_VILO_AUDIT_DIR") or os.environ.get("JAKE_VILO_AUDIT_OUT_DIR") or str(REPO_ROOT / "output" / "vilo_audit"))
TAUC_NYCHA_AUDIT_CSV = Path(os.environ.get("JAKE_TAUC_AUDIT_CSV", str(REPO_ROOT / "output/tauc_nycha_cpe_audit_latest.csv")))
NYCHA_INFO_CSV = next(
    (
        candidate
        for candidate in [
            Path(os.environ["JAKE_NYCHA_INFO_CSV"]) if os.environ.get("JAKE_NYCHA_INFO_CSV") else None,
            REPO_ROOT / "data/nycha_info.csv",
            REPO_ROOT / "output/nycha_info.csv",
            Path.home() / "Downloads/nycha_info.csv",
        ]
        if candidate and candidate.exists()
    ),
    REPO_ROOT / "data/nycha_info.csv",
)
DEVICE_LABEL_RE = re.compile(r"^\d{6}\.\d{3}\.[A-Z]+\d{2}$")

# Deterministic overrides for radios/sites that should not rely on fuzzy location matching.
# Use `None` when the address should remain unresolved until a canonical NetBox prefix exists.
ADDRESS_RESOLUTION_OVERRIDES: dict[str, dict[str, Any] | None] = {
    normalize_free_text("726 Fenimore St, Brooklyn, NY 11203"): None,
    normalize_free_text("225 Buffalo Ave, Brooklyn, NY 11213"): None,
    normalize_free_text("508 Howard Ave, Brooklyn, NY 11233"): None,
    normalize_free_text("545 Ralph Ave, Brooklyn, NY 11233"): None,
    normalize_free_text("1371 St Marks Ave, Brooklyn, NY 11233"): None,
    normalize_free_text("1640 Sterling Pl, Brooklyn, NY 11233"): None,
    normalize_free_text("1691 St Johns Pl, Brooklyn, NY 11233"): None,
    normalize_free_text("1724 Sterling Pl, Brooklyn, NY 11233"): None,
    normalize_free_text("1767 Sterling Pl, Brooklyn, NY 11233"): None,
}

# Explicit topology edges should be avoided unless there is no other authoritative source.
# cnWave peer links are now expected to come from exporter metrics when configured.
def load_radio_link_overrides() -> list[dict[str, Any]]:
    if not NETBOX_RENAME_PROPOSALS_CSV.exists():
        return []
    try:
        with NETBOX_RENAME_PROPOSALS_CSV.open(newline="", encoding="utf-8-sig") as handle:
            rows = list(csv.DictReader(handle))
    except Exception:
        return []

    link_rows = [
        row
        for row in rows
        if str(row.get("site_code") or "") == "000007"
        and str(row.get("model") or "").startswith("EH-")
        and " - " in str(row.get("current_name") or "")
        and str(row.get("confidence") or "").lower() == "high"
        and str(row.get("proposed_prefix") or "").strip()
    ]
    if not link_rows:
        return []

    by_left_label: dict[str, dict[str, str]] = {}
    for row in link_rows:
        left, right = [part.strip() for part in str(row.get("current_name") or "").split(" - ", 1)]
        by_left_label[normalize_free_text(left)] = {
            "left": left,
            "right": right,
            "building_id": canonical_scope(row.get("proposed_prefix")),
            "name": str(row.get("current_name") or ""),
        }

    overrides: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in link_rows:
        left, right = [part.strip() for part in str(row.get("current_name") or "").split(" - ", 1)]
        pair_key = tuple(sorted((normalize_free_text(left), normalize_free_text(right))))
        if pair_key in seen:
            continue
        seen.add(pair_key)
        reverse = by_left_label.get(normalize_free_text(right))
        overrides.append(
            {
                "name": str(row.get("current_name") or f"{left} - {right}"),
                "kind": "cambium",
                "from_name": left,
                "to_name": right,
                "from_building_id": canonical_scope(row.get("proposed_prefix")),
                "to_building_id": reverse.get("building_id") if reverse else None,
                "status": "ok",
                "evidence_source": "rename_sheet_override",
            }
        )
    return overrides


RADIO_LINK_OVERRIDES: list[dict[str, Any]] = load_radio_link_overrides()


def load_customer_port_map() -> dict[str, Any]:
    if not ARTIFACT_PORT_MAP.exists():
        return {"summary": {}, "ports": []}
    return json.loads(ARTIFACT_PORT_MAP.read_text())


def load_transport_radio_scan() -> dict[str, Any]:
    if not ARTIFACT_TRANSPORT_RADIO_SCAN.exists():
        return {"summary": {}, "results": []}
    return json.loads(ARTIFACT_TRANSPORT_RADIO_SCAN.read_text())


def load_tauc_nycha_audit_rows() -> list[dict[str, Any]]:
    if not TAUC_NYCHA_AUDIT_CSV.exists():
        return []
    with TAUC_NYCHA_AUDIT_CSV.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_nycha_info_rows() -> list[dict[str, str]]:
    if not NYCHA_INFO_CSV.exists():
        return []
    with NYCHA_INFO_CSV.open(newline="", encoding="utf-8-sig") as handle:
        data = list(csv.reader(handle))
    if len(data) < 13:
        return []
    header = data[12]
    rows: list[dict[str, str]] = []
    for raw in data[13:]:
        if not raw:
            continue
        rows.append({header[i]: (raw[i] if i < len(raw) else "") for i in range(len(header))})
    return rows


def normalize_address_text(value: str | None) -> str:
    return normalize_free_text(value)


def load_anythingllm_mcp_env(server_name: str) -> dict[str, str]:
    path = Path(
        os.environ.get("ANYTHINGLLM_MCP_SERVERS_JSON")
        or os.environ.get("JAKE_ANYTHINGLLM_MCP_CONFIG")
        or str(Path.home() / "Library" / "Application Support" / "anythingllm-desktop" / "storage" / "plugins" / "anythingllm_mcp_servers.json")
    )
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return (data.get('mcpServers', {}).get(server_name, {}) or {}).get('env', {}) or {}
    except Exception:
        return {}


def getenv_fallback(name: str, server_name: str) -> str:
    return os.environ.get(name, '') or load_anythingllm_mcp_env(server_name).get(name, '') or ''


def load_local_env_file() -> None:
    env_path = Path(os.environ.get("JAKE_ENV_FILE") or os.environ.get("JAKE_LOCAL_ENV_FILE") or str(REPO_ROOT / ".env"))
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


class HttpJSONClient:
    def __init__(self, base_url: str, headers: dict[str, str] | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}

    def request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"
        req = urllib.request.Request(url, headers=self.headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))


class HttpTextClient:
    def __init__(self, base_url: str, headers: dict[str, str] | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}

    def request(self, path: str) -> str:
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, headers=self.headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.read().decode("utf-8", errors="replace")


class ThreadLocalSQLite:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._local = threading.local()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    def execute(self, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
        return self._conn().execute(*args, **kwargs)


PROM_METRIC_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)$")
PROM_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse_prometheus_metrics(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = PROM_METRIC_RE.match(line)
        if not match:
            continue
        name, label_blob, value = match.groups()
        labels: dict[str, str] = {}
        if label_blob:
            for key, label_value in PROM_LABEL_RE.findall(label_blob):
                labels[key] = bytes(label_value, "utf-8").decode("unicode_escape")
        numeric = float(value)
        rows.append({"name": name, "labels": labels, "value": int(numeric) if numeric.is_integer() else numeric})
    return rows


class JakeOps:
    def __init__(self) -> None:
        load_local_env_file()
        db_path = os.environ.get("JAKE_OPS_DB", str(REPO_ROOT / "network_map.db"))
        self.db = ThreadLocalSQLite(db_path)

        self.bigmac = None
        self.alerts = None
        self.netbox = None
        self.cnwave = None
        self.tauc = None
        self.vilo_api = None

        bigmac_url = getenv_fallback("BIGMAC_URL", "bigmac_mcp").rstrip("/")
        bigmac_user = getenv_fallback("BIGMAC_USER", "bigmac_mcp")
        bigmac_password = getenv_fallback("BIGMAC_PASSWORD", "bigmac_mcp")
        if bigmac_url and bigmac_user and bigmac_password:
            token = base64.b64encode(f"{bigmac_user}:{bigmac_password}".encode()).decode()
            self.bigmac = HttpJSONClient(bigmac_url, {"Authorization": f"Basic {token}", "Accept": "application/json"})

        alert_url = getenv_fallback("ALERTMANAGER_URL", "alertmanager_mcp").rstrip("/")
        if alert_url:
            self.alerts = HttpJSONClient(alert_url, {"Accept": "application/json"})

        netbox_url = getenv_fallback("NETBOX_URL", "netbox_mcp").rstrip("/")
        netbox_token = getenv_fallback("NETBOX_TOKEN", "netbox_mcp")
        if netbox_url and netbox_token:
            self.netbox = HttpJSONClient(netbox_url, {"Authorization": f"Token {netbox_token}", "Accept": "application/json"})

        cnwave_exporter_url = getenv_fallback("CNWAVE_EXPORTER_URL", "cnwave_exporter_mcp").rstrip("/")
        if cnwave_exporter_url:
            self.cnwave = HttpTextClient(cnwave_exporter_url, {"Accept": "text/plain"})

        self.tauc = TaucOpsAdapter()
        self.vilo_api = ViloOpsAdapter()

        self._netbox_devices_cache: list[dict[str, Any]] | None = None
        self._location_prefix_index_cache: list[dict[str, Any]] | None = None
        self._site_address_inventory_cache: dict[str, dict[str, Any]] = {}

    def latest_scan_id(self) -> int:
        row = self.db.execute("select max(id) as id from scans").fetchone()
        if not row or row["id"] is None:
            raise ValueError("No scan data found in network_map.db")
        return int(row["id"])

    def latest_scan_meta(self) -> dict[str, Any]:
        row = self.db.execute("select id, started_at, finished_at, subnet, hosts_tested, api_reachable from scans order by id desc limit 1").fetchone()
        return dict(row) if row else {}

    def _device_rows_for_prefix(self, scan_id: int, prefix: str | None) -> list[dict[str, Any]]:
        if prefix:
            rows = self.db.execute(
                "select identity, ip, model, version from devices where scan_id=? and identity like ? order by identity",
                (scan_id, f"{prefix}%"),
            ).fetchall()
        else:
            rows = self.db.execute(
                "select identity, ip, model, version from devices where scan_id=? order by identity",
                (scan_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def _outlier_rows_for_prefix(self, scan_id: int, prefix: str | None) -> list[dict[str, Any]]:
        query = """
            select o.ip, d.identity, o.interface, o.direction, o.severity, o.note
            from one_way_outliers o
            left join devices d on d.scan_id=o.scan_id and d.ip=o.ip
            where o.scan_id=?
        """
        params: list[Any] = [scan_id]
        if prefix:
            query += " and d.identity like ?"
            params.append(f"{prefix}%")
        query += " order by d.identity, o.interface"
        return [dict(r) for r in self.db.execute(query, tuple(params))]

    def _alerts_for_site(self, site_id: str) -> list[dict[str, Any]]:
        if not self.alerts:
            return []
        try:
            return self.alerts.request("/api/v2/alerts", {"active": "true", "filter": [f"site_id={site_id}"]})
        except Exception:
            return []

    def _netbox_all_devices(self) -> list[dict[str, Any]]:
        if self._netbox_devices_cache is not None:
            return self._netbox_devices_cache
        if not self.netbox:
            raise ValueError("NetBox is not configured")
        offset = 0
        limit = 200
        results: list[dict[str, Any]] = []
        while True:
            payload = self.netbox.request("/api/dcim/devices/", {"limit": limit, "offset": offset})
            batch = payload.get("results") or []
            results.extend(batch)
            if not payload.get("next") or not batch:
                break
            offset += limit
        self._netbox_devices_cache = results
        return results

    def _netbox_interface(self, device_name: str, interface_name: str) -> dict[str, Any] | None:
        if not self.netbox:
            return None
        payload = self.netbox.request("/api/dcim/interfaces/", {"device": device_name, "name": interface_name, "limit": 1})
        results = payload.get("results") or []
        return results[0] if results else None

    def _location_prefix_index(self) -> list[dict[str, Any]]:
        if self._location_prefix_index_cache is not None:
            return self._location_prefix_index_cache
        index: dict[tuple[str, str], dict[str, Any]] = {}
        for device in self._netbox_all_devices():
            name = str(device.get("name") or "").strip()
            if not DEVICE_LABEL_RE.match(name):
                continue
            identity = canonical_identity(name)
            if not identity:
                continue
            prefix = ".".join(identity.split(".")[:2])
            location = str((device.get("location") or {}).get("display") or (device.get("location") or {}).get("name") or "").strip()
            if not location:
                continue
            site_code = canonical_scope((device.get("site") or {}).get("slug") or (device.get("site") or {}).get("name"))
            key = (location, prefix)
            row = index.setdefault(
                key,
                {
                    "location": location,
                    "location_norm": normalize_free_text(location),
                    "location_compact": compact_free_text(location),
                    "prefix": prefix,
                    "site_code": site_code,
                    "device_names": [],
                },
            )
            row["device_names"].append(identity)
        self._location_prefix_index_cache = sorted(index.values(), key=lambda x: (x["location"], x["prefix"]))
        return self._location_prefix_index_cache

    def _resolve_building_from_address(self, address_text: str) -> dict[str, Any]:
        addr_norm = normalize_free_text(address_text)
        if addr_norm in ADDRESS_RESOLUTION_OVERRIDES:
            override = ADDRESS_RESOLUTION_OVERRIDES[addr_norm]
            if override is None:
                return {
                    "address_text": address_text,
                    "normalized_query": addr_norm,
                    "resolved": False,
                    "best_match": None,
                    "candidates": [],
                    "override_applied": True,
                }
            return {
                "address_text": address_text,
                "normalized_query": addr_norm,
                "resolved": True,
                "best_match": override,
                "candidates": [override],
                "override_applied": True,
            }
        addr_compact = compact_free_text(address_text)
        query_tokens = [t for t in addr_norm.split() if t]
        candidates: list[dict[str, Any]] = []
        for row in self._location_prefix_index():
            loc_norm = row["location_norm"]
            loc_compact = row["location_compact"]
            score = 0
            if addr_compact and addr_compact in loc_compact:
                score += 100
            elif loc_compact and loc_compact in addr_compact:
                score += 80
            shared = [t for t in query_tokens if t in loc_norm.split()]
            score += len(shared) * 10
            if query_tokens and query_tokens[0].isdigit() and query_tokens[0] in loc_norm.split():
                score += 25
            if len(query_tokens) >= 2 and all(t in loc_norm.split() for t in query_tokens[:2]):
                score += 20
            if score > 0:
                candidates.append(
                    {
                        "location": row["location"],
                        "prefix": row["prefix"],
                        "site_code": row["site_code"],
                        "score": score,
                        "device_names": row["device_names"][:10],
                    }
                )
        candidates.sort(key=lambda x: (-x["score"], x["location"], x["prefix"]))
        best = candidates[0] if candidates else None
        return {
            "address_text": address_text,
            "normalized_query": addr_norm,
            "resolved": bool(best),
            "best_match": best,
            "candidates": candidates[:10],
        }

    def _site_address_inventory(self, site_id: str) -> dict[str, Any]:
        site_id = canonical_scope(site_id)
        cached = self._site_address_inventory_cache.get(site_id)
        if cached is not None:
            return cached

        address_units: dict[str, dict[str, Any]] = {}
        building_units: dict[str, set[str]] = {}
        resolved_address_cache: dict[str, str | None] = {}

        def resolve_building_id_for_address(address: str) -> str | None:
            normalized = str(address or "").strip()
            if not normalized:
                return None
            if normalized not in resolved_address_cache:
                resolved = self._resolve_building_from_address(normalized)
                best = (resolved or {}).get("best_match") or {}
                resolved_prefix = canonical_scope(best.get("prefix"))
                if resolved_prefix and not identity_matches_scope(resolved_prefix, site_id):
                    resolved_prefix = None
                resolved_address_cache[normalized] = resolved_prefix
            return resolved_address_cache[normalized]

        def ensure_address_entry(address: str, building_id: str | None = None) -> dict[str, Any]:
            normalized = str(address or "").strip()
            entry = address_units.setdefault(
                normalized,
                {
                    "address": normalized,
                    "building_id": building_id,
                    "units": set(),
                    "network_names": set(),
                },
            )
            if building_id and not entry.get("building_id"):
                entry["building_id"] = building_id
            return entry

        def add_address_unit(
            address: str,
            unit: str | None,
            network_name: str | None,
            building_id: str | None = None,
        ) -> None:
            normalized_address = str(address or "").strip()
            if not normalized_address:
                return
            resolved_building_id = canonical_scope(building_id) if building_id else resolve_building_id_for_address(normalized_address)
            if resolved_building_id and not identity_matches_scope(resolved_building_id, site_id):
                return
            if not resolved_building_id:
                return
            entry = ensure_address_entry(normalized_address, resolved_building_id)
            if unit:
                entry["units"].add(unit)
                building_units.setdefault(resolved_building_id, set()).add(unit)
            normalized_network_name = str(network_name or "").strip()
            if normalized_network_name:
                entry["network_names"].add(normalized_network_name)

        for row in load_nycha_info_rows():
            address = str(row.get("Address") or "").strip()
            unit = parse_unit_token(row.get("Unit"))
            network_name = str(row.get("PPPoE") or "").strip()
            if address:
                add_address_unit(address, unit, network_name)

        for row in load_tauc_nycha_audit_rows():
            location = str(row.get("expected_location") or "").strip()
            unit = parse_unit_token(row.get("expected_unit"))
            prefix = canonical_scope(row.get("expected_prefix"))
            network_name = str(row.get("networkName") or "").strip()
            if location:
                add_address_unit(location, unit, network_name, prefix)

        # Seed every NetBox-backed site location into the inventory even when
        # there is no NYCHA/TAUC unit evidence yet. This keeps switch-only
        # sites present in the topology/map.
        for row in self._location_prefix_index():
            prefix = canonical_scope(row.get("prefix"))
            if not prefix or not identity_matches_scope(prefix, site_id):
                continue
            location = str(row.get("location") or "").strip()
            if not location:
                continue
            ensure_address_entry(location, prefix)

        inventory = {
            "address_units": address_units,
            "building_units": building_units,
        }
        self._site_address_inventory_cache[site_id] = inventory
        return inventory

    def _label_audit_rows(self, rows: list[dict[str, Any]], source: str) -> dict[str, Any]:
        invalid = []
        valid = []
        for row in rows:
            name = str(row.get("name") or row.get("identity") or "").strip()
            item = {"name": name, "source": source}
            if "ip" in row:
                item["ip"] = row.get("ip")
            if "id" in row:
                item["id"] = row.get("id")
            if DEVICE_LABEL_RE.match(name):
                valid.append(item)
            else:
                invalid.append(item)
        return {"total": len(rows), "valid": valid, "invalid": invalid}

    def get_server_info(self) -> dict[str, Any]:
        return {
            "latest_scan": self.latest_scan_meta(),
            "bigmac_configured": self.bigmac is not None,
            "alertmanager_configured": self.alerts is not None,
            "netbox_configured": self.netbox is not None,
            "cnwave_exporter_configured": self.cnwave is not None,
            "tauc": {
                **(self.tauc.summary() if self.tauc else {"cloud_configured": False, "acs_configured": False, "olt_configured": False}),
            },
            "vilo": self.vilo_api.summary() if self.vilo_api else {"configured": False},
            "tools": [tool["name"] for tool in TOOLS],
        }

    def _tauc_summary(self) -> dict[str, Any]:
        return self.tauc.summary() if self.tauc else {"cloud_configured": False, "acs_configured": False, "olt_configured": False}

    def _vilo_summary(self) -> dict[str, Any]:
        return self.vilo_api.summary() if self.vilo_api else {"configured": False}

    def _cnwave_metrics(self) -> list[dict[str, Any]]:
        if not self.cnwave:
            return []
        try:
            return parse_prometheus_metrics(self.cnwave.request("/metrics"))
        except Exception:
            return []

    def _cnwave_site_summary(self, site_id: str) -> dict[str, Any]:
        rows = self._cnwave_metrics()
        if not rows:
            return {"configured": self.cnwave is not None, "available": False}
        scoped = [r for r in rows if str(r.get("labels", {}).get("site_id", "")) == str(site_id)]
        device_status = [r for r in scoped if r["name"] == "cnwave_device_status"]
        link_status = [r for r in scoped if r["name"] == "cnwave_link_status"]
        device_alarms = [r for r in scoped if r["name"] == "cnwave_device_alarms"]
        down_devices = [r for r in device_status if float(r["value"]) < 1]
        down_links = [r for r in link_status if float(r["value"]) < 1]
        return {
            "configured": True,
            "available": True,
            "site_id": site_id,
            "device_rows": len(device_status),
            "device_up": sum(1 for r in device_status if float(r["value"]) >= 1),
            "device_down": len(down_devices),
            "link_rows": len(link_status),
            "link_up": sum(1 for r in link_status if float(r["value"]) >= 1),
            "link_down": len(down_links),
            "alarm_total": sum(int(float(r["value"])) for r in device_alarms),
            "down_device_names": sorted({r.get("labels", {}).get("name") for r in down_devices if r.get("labels", {}).get("name")})[:20],
            "down_link_names": sorted({r.get("labels", {}).get("link_name") for r in down_links if r.get("labels", {}).get("link_name")})[:20],
        }

    def _cnwave_site_links(self, site_id: str) -> list[dict[str, Any]]:
        rows = self._cnwave_metrics()
        links: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def add_link(link: dict[str, Any]) -> None:
            left = str(link.get("from_label") or "").strip()
            right = str(link.get("to_label") or "").strip()
            if not left or not right:
                return
            dedupe_key = tuple(sorted((normalize_free_text(left), normalize_free_text(right))))
            if dedupe_key in seen:
                return
            seen.add(dedupe_key)
            links.append(link)

        if rows:
            scoped = [
                r
                for r in rows
                if r["name"] == "cnwave_link_status" and str(r.get("labels", {}).get("site_id", "")) == str(site_id)
            ]
            if scoped:
                def labels_for(row: dict[str, Any]) -> dict[str, Any]:
                    return row.get("labels", {}) or {}

                def name_pair_from_labels(labels: dict[str, Any]) -> tuple[str | None, str | None]:
                    candidate_pairs = [
                        ("from_name", "to_name"),
                        ("src_name", "dst_name"),
                        ("source_name", "target_name"),
                        ("local_name", "remote_name"),
                        ("a_name", "z_name"),
                        ("node_a_name", "node_z_name"),
                        ("dn_name", "cn_name"),
                        ("pop_name", "cn_name"),
                        ("name", "peer_name"),
                    ]
                    for left_key, right_key in candidate_pairs:
                        left = str(labels.get(left_key) or "").strip()
                        right = str(labels.get(right_key) or "").strip()
                        if left and right:
                            return left, right

                    link_name = str(labels.get("link_name") or labels.get("name") or "").strip()
                    for sep in (" <-> ", " -> ", " - ", " to "):
                        if sep in link_name:
                            left, right = [part.strip() for part in link_name.split(sep, 1)]
                            if left and right:
                                return left, right
                    return None, None

                for row in scoped:
                    labels = labels_for(row)
                    left, right = name_pair_from_labels(labels)
                    if not left or not right:
                        continue
                    add_link(
                        {
                            "name": str(labels.get("link_name") or f"{left} - {right}"),
                            "kind": "cambium",
                            "from_label": left,
                            "to_label": right,
                            "status": "ok" if float(row.get("value") or 0) >= 1 else "down",
                            "metric_labels": labels,
                            "evidence_source": "cnwave_exporter",
                        }
                    )

        if links:
            return links

        if canonical_scope(site_id) != "000007":
            return []

        radio_scan = load_transport_radio_scan()
        mac_to_name: dict[str, str] = {}
        neighbors_by_name: dict[str, set[str]] = {}
        for row in radio_scan.get("results") or []:
            if row.get("type") != "cambium" or row.get("status") != "ok":
                continue
            name = str(row.get("name") or "").strip()
            if not name:
                continue
            seen_macs = {
                norm_mac(str(value))
                for value in [row.get("device_mac"), *(row.get("wlan_macs") or []), *(row.get("initiator_macs") or [])]
                if str(value or "").strip()
            }
            for mac in seen_macs:
                mac_to_name[mac] = name
            neighbors_by_name[name] = {
                norm_mac(str(value))
                for value in (row.get("neighbor_macs") or [])
                if str(value or "").strip()
            }

        for name, neighbor_macs in neighbors_by_name.items():
            for neighbor_mac in neighbor_macs:
                peer_name = mac_to_name.get(neighbor_mac)
                if not peer_name or peer_name == name:
                    continue
                add_link(
                    {
                        "name": f"{name} - {peer_name}",
                        "kind": "cambium",
                        "from_label": name,
                        "to_label": peer_name,
                        "status": "ok",
                        "evidence_source": "transport_scan_neighbor",
                    }
                )

        return links

    def query_summary(self, query: str) -> dict[str, Any]:
        from packages.jake.queries.jake_query_core import run_operator_query

        return run_operator_query(self, query)

    def get_outage_context(self, address_text: str, unit: str) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        unit_norm = normalize_free_text(unit).replace("unit ", "").replace("apt ", "").replace("apartment ", "").strip()
        target_unit_token = parse_unit_token(unit_norm)
        target_floor, target_letter = parse_unit_parts(unit_norm)
        building = self._resolve_building_from_address(address_text)
        best = building.get("best_match") or {}
        building_id = canonical_scope(best.get("prefix"))
        site_id = canonical_scope(best.get("site_code")) if best.get("site_code") else (building_id.split(".")[0] if building_id else None)

        sessions_for_address: list[dict[str, Any]] = []
        exact_unit_sessions: list[dict[str, Any]] = []
        if building_id:
            router_prefix = canonical_scope(site_id)
            ppp_rows = [
                dict(r)
                for r in self.db.execute(
                    """
                    select p.router_ip, p.name, p.service, p.caller_id, p.address, p.uptime, d.identity
                    from router_ppp_active p
                    left join devices d on d.scan_id=p.scan_id and d.ip=p.router_ip
                    where p.scan_id=? and d.identity like ?
                    order by p.name
                    """,
                    (scan_id, f"{router_prefix}%"),
                ).fetchall()
            ]
            building_tokens = [t for t in compact_free_text(address_text).split() if t]
            address_compact = compact_free_text(address_text)
            for row in ppp_rows:
                name = str(row.get("name") or "")
                name_compact = compact_free_text(name)
                if address_compact and address_compact in name_compact:
                    sessions_for_address.append(row)
                    if unit_norm and compact_free_text(unit_norm) in name_compact:
                        exact_unit_sessions.append(row)

        address_caller_ids = sorted({norm_mac(r.get("caller_id") or "") for r in sessions_for_address if r.get("caller_id")})
        bridge_hits: list[dict[str, Any]] = []
        bridge_hits_by_mac: dict[str, list[dict[str, Any]]] = {}
        if address_caller_ids:
            placeholders = ",".join("?" for _ in address_caller_ids)
            all_bridge_hits = [
                dict(r)
                for r in self.db.execute(
                    f"""
                    select d.identity, d.ip, bh.on_interface, bh.vid, bh.mac, bh.local, bh.external
                    from bridge_hosts bh
                    left join devices d on d.scan_id=bh.scan_id and d.ip=bh.ip
                    where bh.scan_id=? and lower(bh.mac) in ({placeholders})
                    order by d.identity, bh.on_interface
                    """,
                    [scan_id, *address_caller_ids],
                ).fetchall()
            ]
            for row in all_bridge_hits:
                mac = norm_mac(row.get("mac") or "")
                bridge_hits_by_mac.setdefault(mac, []).append(row)
            exact_caller_ids = {norm_mac(r.get("caller_id") or "") for r in exact_unit_sessions if r.get("caller_id")}
            bridge_hits = [row for row in all_bridge_hits if norm_mac(row.get("mac") or "") in exact_caller_ids]

        same_address_edge_context: list[dict[str, Any]] = []
        for row in sessions_for_address:
            caller_id = norm_mac(row.get("caller_id") or "")
            hits = bridge_hits_by_mac.get(caller_id, [])
            same_address_edge_context.append(
                {
                    "name": row.get("name"),
                    "unit_token": parse_unit_token(row.get("name")),
                    "caller_id": caller_id,
                    "best_bridge_hit": best_bridge_hit(hits),
                    "all_bridge_hits": hits[:10],
                }
            )

        port_rows = self._port_map_scope_rows(building_id=building_id) if building_id else []
        unit_comment_rows = []
        if unit_norm:
            unit_tokens = {unit_norm, f"unit {unit_norm}", unit_norm.replace(" ", "")}
            for row in port_rows:
                comment = normalize_free_text(row.get("comment"))
                if comment and any(token in comment for token in unit_tokens):
                    unit_comment_rows.append(row)

        neighboring_unit_port_hints = []
        if target_unit_token:
            target_floor_match = re.match(r"(\d+)", target_unit_token)
            target_floor_value = target_floor_match.group(1) if target_floor_match else None
            for row in same_address_edge_context:
                unit_token = row.get("unit_token")
                if not unit_token:
                    continue
                if unit_token == target_unit_token:
                    continue
                if target_floor_value and not str(unit_token).startswith(target_floor_value):
                    continue
                if row.get("best_bridge_hit"):
                    neighboring_unit_port_hints.append(row)

        inferred_unit_port_candidates = infer_unit_port_candidates(
            target_unit_token,
            target_floor,
            target_letter,
            neighboring_unit_port_hints,
            unit_comment_rows,
        )

        likely_causes: list[dict[str, Any]] = []
        suggested_checks: list[dict[str, Any]] = []
        if building_id and not exact_unit_sessions:
            likely_causes.append(
                {
                    "type": "single_unit_service_loss",
                    "confidence": "high" if sessions_for_address else "medium",
                    "reason": "The reported unit has no active PPP session while other units at the same address do, which points away from a whole-building outage.",
                }
            )
            suggested_checks.extend(
                [
                    {
                        "priority": 1,
                        "category": "physical_layer",
                        "check": "Verify the CPE has power and the WAN/LAN link LEDs are lit. Reseat or replace the patch cable and inspect the wall jack.",
                    },
                    {
                        "priority": 2,
                        "category": "cpe_mode",
                        "check": "Confirm the CPE is in router/WAN mode and not AP mode. A common AP-mode sign is the client MAC differing from the expected CPE MAC only in the last digit or last two hex digits.",
                    },
                    {
                        "priority": 3,
                        "category": "wan_config",
                        "check": "If this service is PPPoE-backed, confirm the CPE WAN is set to PPPoE and not DHCP. If DHCP is seen on VLAN 20 where PPPoE is expected, treat that as misconfiguration or fallback behavior.",
                    },
                    {
                        "priority": 4,
                        "category": "rogue_dhcp",
                        "check": "If the client is receiving the wrong address family, check for a rogue DHCP server. A common sign on TP-Link CPEs is the first octet pair shifting from 30: to 32:, which often indicates locally administered MAC behavior on a misbehaving DHCP-serving CPE.",
                    },
                ]
            )
        if inferred_unit_port_candidates:
            candidate_ports = ", ".join(f"{c['identity']} {c['on_interface']}" for c in inferred_unit_port_candidates[:3])
            likely_causes.append(
                {
                    "type": "probable_local_edge_port_issue",
                    "confidence": "medium",
                    "reason": f"Adjacent same-floor units are online and map to nearby ports, so {target_unit_token} likely lands on a neighboring access port that can be field-checked directly.",
                }
            )
            suggested_checks.insert(
                1,
                {
                    "priority": 1,
                    "category": "edge_port",
                    "check": f"Field-check the inferred access port candidate(s): {candidate_ports}. Look for link, flap history, and whether the wrong device is patched there.",
                },
            )
        if any(alert.get("labels", {}).get("severity") == "critical" for alert in (self._alerts_for_site(site_id) if self.alerts and site_id else [])):
            likely_causes.append(
                {
                    "type": "site_alert_present_but_not_unit_specific",
                    "confidence": "low",
                    "reason": "There is an active site alert, but it does not currently identify the reported unit or building as the failed path.",
                }
            )

        netbox_physical_context: list[dict[str, Any]] = []
        for candidate in inferred_unit_port_candidates[:3]:
            device_name = str(candidate.get("identity") or "")
            interface_name = str(candidate.get("on_interface") or "")
            device_payload = self.get_netbox_device(device_name) if self.netbox and device_name else {}
            device_results = device_payload.get("results") or []
            device = device_results[0] if device_results else None
            iface = self._netbox_interface(device_name, interface_name) if self.netbox and device_name and interface_name else None
            netbox_physical_context.append(
                {
                    "device_name": device_name,
                    "interface_name": interface_name,
                    "device_location": ((device or {}).get("location") or {}).get("display"),
                    "device_primary_ip4": ((device or {}).get("primary_ip4") or {}).get("address"),
                    "interface_label": (iface or {}).get("label"),
                    "interface_type": (((iface or {}).get("type") or {}).get("label")),
                    "interface_enabled": (iface or {}).get("enabled"),
                    "interface_occupied": (iface or {}).get("_occupied"),
                    "cable_present": bool((iface or {}).get("cable")),
                    "connected_endpoints": (iface or {}).get("connected_endpoints"),
                }
            )

        plain_english_summary = (
            f"{address_text.title()} unit {target_unit_token or unit.upper()} is not currently online. "
            f"The building resolved to {building_id or 'unknown'}, and other units at the same address are online, so this looks more like a unit-level issue than a whole-building outage."
        )
        if inferred_unit_port_candidates:
            plain_english_summary += (
                f" Based on nearby same-floor units, the most likely access port is "
                f"{inferred_unit_port_candidates[0]['identity']} {inferred_unit_port_candidates[0]['on_interface']}."
            )
        if netbox_physical_context:
            top_ctx = netbox_physical_context[0]
            cable_text = "has a NetBox cable record" if top_ctx.get("cable_present") else "does not currently have a NetBox cable record"
            plain_english_summary += f" NetBox shows that port at {top_ctx.get('device_location') or 'the switch location'} and it {cable_text}."

        return {
            "address_text": address_text,
            "unit": unit,
            "resolution": building,
            "building_id": building_id,
            "site_id": site_id,
            "exact_unit_online": bool(exact_unit_sessions),
            "exact_unit_sessions": exact_unit_sessions[:25],
            "same_address_online_sessions": sessions_for_address[:50],
            "exact_unit_bridge_hits": bridge_hits[:25],
            "same_address_edge_context": same_address_edge_context[:50],
            "neighboring_unit_port_hints": neighboring_unit_port_hints[:25],
            "inferred_unit_port_candidates": inferred_unit_port_candidates[:10],
            "netbox_physical_context": netbox_physical_context,
            "unit_comment_matches": unit_comment_rows[:25],
            "active_alerts": self._alerts_for_site(site_id) if self.alerts and site_id else [],
            "plain_english_summary": plain_english_summary,
            "likely_causes": likely_causes,
            "suggested_checks": sorted(suggested_checks, key=lambda x: (x["priority"], x["category"])),
            "notes": {
                "unit_mapping_complete": bool(unit_comment_rows or bridge_hits),
                "ppp_name_match_method": "compact address/unit substring match against live router_ppp_active names",
                "bridge_match_method": "caller_id MACs from exact unit PPP sessions correlated against latest bridge_hosts snapshot",
                "neighboring_unit_port_hint_method": "same-address online PPP sessions correlated to latest bridge_hosts; edge ports preferred over uplinks",
            },
        }

    def audit_device_labels(self, include_valid: bool = False, limit: int = 500) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        network_rows = [
            dict(r)
            for r in self.db.execute(
                "select distinct identity, ip from devices where scan_id=? and identity is not null and trim(identity) != '' order by identity",
                (scan_id,),
            ).fetchall()
        ]
        netbox_rows = [{"name": d.get("name"), "id": d.get("id")} for d in self._netbox_all_devices() if d.get("name")]

        network = self._label_audit_rows(network_rows, "network")
        netbox = self._label_audit_rows(netbox_rows, "netbox")
        invalid_unique = sorted({row["name"] for row in [*network["invalid"], *netbox["invalid"]]})

        result = {
            "pattern": DEVICE_LABEL_RE.pattern,
            "rule": "<6 digit location>.<3 digit site>.<device type><2 digit number>",
            "network": {
                "total": network["total"],
                "invalid_count": len(network["invalid"]),
                "invalid": network["invalid"][:limit],
            },
            "netbox": {
                "total": netbox["total"],
                "invalid_count": len(netbox["invalid"]),
                "invalid": netbox["invalid"][:limit],
            },
            "combined_invalid_unique_count": len(invalid_unique),
            "combined_invalid_unique": invalid_unique[:limit],
        }
        if include_valid:
            result["network"]["valid"] = network["valid"][:limit]
            result["netbox"]["valid"] = netbox["valid"][:limit]
        return result

    def get_subnet_health(self, subnet: str | None, site_id: str | None, include_alerts: bool, include_bigmac: bool) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        site_prefix = site_id or ("000007" if subnet == "192.168.44.0/24" else None)

        devices = self._device_rows_for_prefix(scan_id, site_prefix)
        outliers = self._outlier_rows_for_prefix(scan_id, site_prefix)

        result: dict[str, Any] = {
            "verified": {
                "scan": self.latest_scan_meta(),
                "device_count": len(devices),
                "outlier_count": len(outliers),
                "devices": devices[:100],
                "outliers": outliers[:100],
            },
            "inferred": [],
        }
        if include_alerts and self.alerts and site_prefix:
            result["verified"]["active_alerts"] = self._alerts_for_site(site_prefix)
        if include_bigmac and self.bigmac and site_prefix:
            result["verified"]["bigmac_stats"] = self.bigmac.request("/api/stats")
        if outliers:
            result["inferred"].append("one_way_outliers_present")
        return result

    def get_online_customers(self, scope: str | None, site_id: str | None, building_id: str | None, router_identity: str | None) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        if not site_id and scope and scope.startswith("000"):
            site_id = scope
        if not building_id and scope and scope.count(".") >= 1 and scope != site_id:
            building_id = scope
        if not router_identity and scope and re.search(r"\.R\d{1,2}$", scope, re.IGNORECASE):
            router_identity = scope

        # Resolve routers from actual PPP activity rather than assuming an old naming pattern
        # such as *.R1. This keeps counts working after identity normalization to *.R01.
        sessions = [
            dict(r)
            for r in self.db.execute(
                """
                select p.router_ip, p.name, p.service, p.caller_id, p.address, p.uptime, d.identity
                from router_ppp_active p
                left join devices d on d.scan_id=p.scan_id and d.ip=p.router_ip
                where p.scan_id=?
                order by d.identity, p.name
                """,
                (scan_id,),
            ).fetchall()
        ]
        routers_map: dict[tuple[str, str], dict[str, Any]] = {}
        filtered_sessions: list[dict[str, Any]] = []
        canonical_router_identity = canonical_identity(router_identity) if router_identity else None
        canonical_site_id = canonical_scope(site_id) if site_id else None
        canonical_building_id = canonical_scope(building_id) if building_id else None
        for session in sessions:
            identity = canonical_identity(session.get("identity"))
            router_ip = session.get("router_ip")
            if not identity or not router_ip:
                continue
            if canonical_router_identity and identity != canonical_router_identity:
                continue
            if canonical_building_id and not identity_matches_scope(identity, canonical_building_id):
                continue
            if not canonical_building_id and canonical_site_id and not identity_matches_scope(identity, canonical_site_id):
                continue
            filtered_sessions.append(session)
            routers_map[(identity, router_ip)] = {"identity": identity, "ip": router_ip}
        routers = list(routers_map.values())

        if not routers:
            return {"count": 0, "counting_method": "router_ppp_active", "matched_routers": [], "verified_sessions": [], "error": "No matching router found in latest scan for requested scope"}
        return {
            "count": len(filtered_sessions),
            "counting_method": "router_ppp_active",
            "matched_routers": routers,
            "verified_sessions": filtered_sessions[:500],
        }

    def trace_mac(self, mac: str, include_bigmac: bool) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        mac = norm_mac(mac)
        rows = self.db.execute(
            """
            select bh.ip, d.identity, bh.mac, bh.on_interface, bh.vid, bh.local, bh.external
            from bridge_hosts bh
            left join devices d on d.scan_id=bh.scan_id and d.ip=bh.ip
            where bh.scan_id=? and lower(bh.mac)=lower(?)
            order by case when bh.on_interface like 'ether%' then 0 else 1 end, bh.external desc, bh.local asc, d.identity
            """,
            (scan_id, mac),
        ).fetchall()
        sightings = [dict(r) for r in rows]
        result: dict[str, Any] = {"mac": mac, "verified_sightings": sightings}
        if rows:
            best = dict(rows[0])
            result["best_guess"] = best
            neighbors = self.db.execute(
                "select interface, neighbor_identity, neighbor_address, platform, version from neighbors where scan_id=? and ip=? order by interface",
                (scan_id, best["ip"]),
            ).fetchall()
            result["neighbor_context"] = [dict(r) for r in neighbors]
        else:
            result["best_guess"] = None
            result["neighbor_context"] = []
        if include_bigmac and self.bigmac:
            try:
                result["bigmac_corroboration"] = self.bigmac.request("/api/search", {"mac": mac.replace(':', '')})
            except Exception as exc:
                result["bigmac_corroboration_error"] = str(exc)
        edge_sightings = [s for s in sightings if is_edge_port(s.get("on_interface"))]
        uplink_sightings = [s for s in sightings if is_uplink_like_port(s.get("on_interface"))]
        bigmac_rows = []
        if isinstance(result.get("bigmac_corroboration"), dict):
            bigmac_rows = result["bigmac_corroboration"].get("results") or []
        bigmac_edge = [r for r in bigmac_rows if is_edge_port(r.get("port_name") or r.get("interface_name"))]
        bigmac_uplink = [r for r in bigmac_rows if is_uplink_like_port(r.get("port_name") or r.get("interface_name"))]

        best_bigmac_edge_guess = None
        if bigmac_edge:
            best_bigmac_edge_guess = sorted(bigmac_edge, key=lambda r: str(r.get("last_seen") or ""), reverse=True)[0]
            result["bigmac_best_edge_guess"] = best_bigmac_edge_guess

        if edge_sightings:
            result["trace_status"] = "edge_trace_found"
            result["reason"] = "A current latest-scan bridge-host sighting exists on an access port."
        elif sightings:
            result["trace_status"] = "latest_scan_uplink_only"
            result["reason"] = "The MAC is visible in the latest scan, but only on uplink or non-edge interfaces."
        elif bigmac_edge:
            result["trace_status"] = "bigmac_edge_corroboration_only"
            result["reason"] = "No latest-scan sighting exists, but Bigmac has cached edge-port corroboration."
        elif bigmac_uplink or bigmac_rows:
            result["trace_status"] = "upstream_or_cached_corroboration_only"
            result["reason"] = "No latest-scan sighting exists; only upstream or cached Bigmac corroboration is available."
        else:
            result["trace_status"] = "not_found_in_latest_scan"
            result["reason"] = "No matching bridge-host sighting for this MAC exists in the latest local scan."
        result["edge_sighting_count"] = len(edge_sightings)
        result["uplink_sighting_count"] = len(uplink_sightings)
        return result

    def get_netbox_device(self, name: str) -> dict[str, Any]:
        if not self.netbox:
            raise ValueError("NetBox is not configured")
        return self.netbox.request("/api/dcim/devices/", {"name": name, "limit": 1})

    def get_site_alerts(self, site_id: str) -> dict[str, Any]:
        if not self.alerts:
            raise ValueError("Alertmanager is not configured")
        alerts = self._alerts_for_site(site_id)
        return {"site_id": site_id, "alerts": alerts, "count": len(alerts)}

    def get_site_summary(self, site_id: str, include_alerts: bool) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        devices = self._device_rows_for_prefix(scan_id, site_id)
        routers = [d for d in devices if re.search(r"\.R\d{1,2}$", str(d["identity"])) is not None]
        switches = [d for d in devices if ".SW" in d["identity"] or ".RFSW" in d["identity"]]
        outliers = self._outlier_rows_for_prefix(scan_id, site_id)
        online = self.get_online_customers(site_id, site_id, None, None)
        bridge_counts = self.db.execute(
            """
            select count(*) as total,
                   sum(case when lower(mac) like '30:68:93%' or lower(mac) like '60:83:e7%' or lower(mac) like '7c:f1:7e%' or lower(mac) like 'd8:44:89%' or lower(mac) like 'dc:62:79%' or lower(mac) like 'e4:fa:c4%' then 1 else 0 end) as tplink,
                   sum(case when lower(mac) like 'e8:da:00%' then 1 else 0 end) as vilo
            from bridge_hosts bh
            left join devices d on d.scan_id=bh.scan_id and d.ip=bh.ip
            where bh.scan_id=? and d.identity like ?
            """,
            (scan_id, f"{site_id}%"),
        ).fetchone()
        result = {
            "site_id": site_id,
            "scan": self.latest_scan_meta(),
            "devices_total": len(devices),
            "routers": routers,
            "switches_count": len(switches),
            "online_customers": {"count": online["count"], "counting_method": online["counting_method"], "matched_routers": online["matched_routers"]},
            "outlier_count": len(outliers),
            "bridge_host_summary": dict(bridge_counts) if bridge_counts else {},
        }
        result["cnwave_summary"] = self._cnwave_site_summary(site_id)
        result["tauc_summary"] = self._tauc_summary()
        result["vilo_summary"] = self._vilo_summary()
        if include_alerts and self.alerts:
            result["active_alerts"] = self._alerts_for_site(site_id)
        return result

    def get_site_topology(self, site_id: str) -> dict[str, Any]:
        radio_scan = load_transport_radio_scan()
        alerts = self._alerts_for_site(site_id) if self.alerts else []
        address_inventory = self._site_address_inventory(site_id)
        address_units: dict[str, dict[str, Any]] = address_inventory["address_units"]
        building_units: dict[str, set[str]] = address_inventory["building_units"]

        radios: list[dict[str, Any]] = []
        radio_links: list[dict[str, Any]] = []
        radio_name_to_building_id: dict[str, str] = {}
        siklu_label_to_building_id: dict[str, str] = {}
        address_coords: dict[str, tuple[float, float]] = {}
        building_coords: dict[str, tuple[float, float]] = {}
        for row in radio_scan.get("results") or []:
            location = str(row.get("location") or "").strip()
            resolved = self._resolve_building_from_address(location) if location else {"resolved": False, "best_match": None}
            best = resolved.get("best_match") or {}
            building_id = canonical_scope(best.get("prefix"))
            matching_alerts = [
                alert for alert in alerts
                if normalize_free_text(str(((alert.get("annotations") or {}).get("device_name")) or ((alert.get("labels") or {}).get("name")) or ""))
                == normalize_free_text(str(row.get("name") or ""))
            ]
            name = str(row.get("name") or "")
            model = str(row.get("model") or "")
            latitude = row.get("latitude")
            longitude = row.get("longitude")
            try:
                lat_value = float(latitude) if latitude is not None else None
                lon_value = float(longitude) if longitude is not None else None
            except (TypeError, ValueError):
                lat_value = None
                lon_value = None
            if lat_value is not None and lon_value is not None:
                address_coords[location] = (lat_value, lon_value)
                if building_id:
                    building_coords[building_id] = (lat_value, lon_value)
            if row.get("type") == "siklu" and " - " in name:
                left, right = [part.strip() for part in name.split(" - ", 1)]
                radio_links.append(
                    {
                        "name": name,
                        "kind": "siklu",
                        "from_label": left,
                        "to_label": right,
                        "from_building_id": building_id,
                        "status": row.get("status"),
                        "ip": row.get("ip"),
                        "location": location,
                        "evidence_source": "siklu_transport_scan",
                    }
                )
                if building_id:
                    siklu_label_to_building_id[normalize_free_text(left)] = building_id
            if building_id:
                radio_name_to_building_id[name] = building_id
            radios.append(
                {
                    "name": name,
                    "type": row.get("type"),
                    "model": model,
                    "ip": row.get("ip"),
                    "location": location,
                    "status": row.get("status"),
                    "resolved_building_id": building_id,
                    "resolved_building_match": best,
                    "address_units": sorted((address_units.get(location, {}) or {}).get("units") or []),
                    "network_names": sorted((address_units.get(location, {}) or {}).get("network_names") or []),
                    "latitude": lat_value,
                    "longitude": lon_value,
                    "coordinate_source": row.get("coordinate_source"),
                    "alert_count": len(matching_alerts),
                    "alerts": matching_alerts[:10],
                }
            )

        for link in radio_links:
            if str(link.get("kind") or "").lower() != "siklu":
                continue
            if not link.get("from_building_id"):
                link["from_building_id"] = siklu_label_to_building_id.get(
                    normalize_free_text(str(link.get("from_label") or ""))
                )
            if not link.get("to_building_id"):
                link["to_building_id"] = siklu_label_to_building_id.get(
                    normalize_free_text(str(link.get("to_label") or ""))
                )

        for link in self._cnwave_site_links(site_id):
            radio_links.append(
                {
                    **link,
                    "from_building_id": radio_name_to_building_id.get(str(link.get("from_label") or "")),
                    "to_building_id": radio_name_to_building_id.get(str(link.get("to_label") or "")),
                }
            )

        for override in RADIO_LINK_OVERRIDES:
            radio_links.append(
                {
                    "name": override["name"],
                    "kind": override["kind"],
                    "from_label": override.get("from_name"),
                    "to_label": override.get("to_name"),
                    "from_building_id": override.get("from_building_id") or radio_name_to_building_id.get(str(override.get("from_name") or "")),
                    "to_building_id": override.get("to_building_id") or radio_name_to_building_id.get(str(override.get("to_name") or "")),
                    "status": override.get("status"),
                    "evidence_source": override.get("evidence_source"),
                }
            )

        source_rank = {
            "siklu_transport_scan": 0,
            "cnwave_exporter": 0,
            "transport_scan_neighbor": 1,
            "rename_sheet_override": 2,
        }

        def normalized_link_label(label: str) -> str:
            cleaned = re.sub(r"\s+(v\d{3,}|eh-\S+)$", "", str(label or "").strip(), flags=re.IGNORECASE)
            return normalize_free_text(cleaned)

        deduped_links: list[dict[str, Any]] = []
        link_index_by_pair: dict[tuple[str, str], int] = {}
        for link in radio_links:
            left = normalized_link_label(str(link.get("from_label") or ""))
            right = normalized_link_label(str(link.get("to_label") or ""))
            if not left or not right:
                deduped_links.append(link)
                continue
            pair_key = tuple(sorted((left, right)))
            rank = source_rank.get(str(link.get("evidence_source") or ""), 3)
            existing_index = link_index_by_pair.get(pair_key)
            if existing_index is None:
                link_index_by_pair[pair_key] = len(deduped_links)
                deduped_links.append(link)
                continue
            existing_rank = source_rank.get(str(deduped_links[existing_index].get("evidence_source") or ""), 3)
            if rank < existing_rank:
                deduped_links[existing_index] = link
        radio_links = deduped_links

        buildings: list[dict[str, Any]] = []
        seen_buildings = sorted(
            {
                canonical_scope(entry.get("building_id"))
                for entry in address_units.values()
                if entry.get("building_id")
            }
            | {
                canonical_scope(r.get("resolved_building_id"))
                for r in radios
                if r.get("resolved_building_id")
            }
            | set(building_units.keys())
        )
        for building_id in seen_buildings:
            buildings.append(
                {
                    "building_id": building_id,
                    "customer_count": self.get_building_customer_count(building_id).get("count", 0),
                    "health": self.get_building_health(building_id, include_alerts=False),
                    "known_units": sorted(building_units.get(building_id, set())),
                    "latitude": building_coords.get(building_id, (None, None))[0],
                    "longitude": building_coords.get(building_id, (None, None))[1],
                }
            )

        return {
            "site_id": site_id,
            "scan": self.latest_scan_meta(),
            "radio_scan_summary": radio_scan.get("summary") or {},
            "radios": radios,
            "radio_links": radio_links,
            "addresses": [
                {
                    "address": address,
                    "building_id": entry.get("building_id"),
                    "units": sorted(entry.get("units") or []),
                    "network_names": sorted(entry.get("network_names") or []),
                    "latitude": address_coords.get(address, (None, None))[0],
                    "longitude": address_coords.get(address, (None, None))[1],
                }
                for address, entry in sorted(address_units.items())
            ],
            "buildings": buildings,
        }

    def get_building_health(self, building_id: str, include_alerts: bool) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        devices = self._device_rows_for_prefix(scan_id, building_id)
        outliers = self._outlier_rows_for_prefix(scan_id, building_id)
        host_rows = self.db.execute(
            """
            select d.identity, bh.ip, bh.on_interface, bh.vid, bh.mac, bh.local, bh.external
            from bridge_hosts bh
            left join devices d on d.scan_id=bh.scan_id and d.ip=bh.ip
            where bh.scan_id=? and d.identity like ?
            order by d.identity, bh.on_interface
            """,
            (scan_id, f"{building_id}%"),
        ).fetchall()
        probable_cpes = [dict(r) for r in host_rows if is_probable_customer_bridge_host(dict(r))]
        result = {
            "building_id": building_id,
            "scan": self.latest_scan_meta(),
            "device_count": len(devices),
            "devices": devices,
            "outlier_count": len(outliers),
            "outliers": outliers[:100],
            "probable_cpe_count": len(probable_cpes),
            "probable_cpes": probable_cpes[:200],
            "tauc_summary": self._tauc_summary(),
            "vilo_summary": self._vilo_summary(),
        }
        site_id = building_id.split(".")[0]
        if include_alerts and self.alerts and site_id:
            result["active_alerts"] = self._alerts_for_site(site_id)
        return result

    def _building_address_record(self, building_id: str) -> dict[str, Any] | None:
        site_id = canonical_scope(building_id.split(".")[0])
        inventory = self._site_address_inventory(site_id)
        canonical_building_id = canonical_scope(building_id)
        for address, entry in sorted(inventory["address_units"].items()):
            if canonical_scope(entry.get("building_id")) != canonical_building_id:
                continue
            return {
                "address": address,
                "building_id": canonical_building_id,
                "units": sorted(entry.get("units") or []),
                "network_names": sorted(entry.get("network_names") or []),
                "latitude": None,
                "longitude": None,
            }
        return None

    def _exact_unit_port_matches(self, building_id: str) -> list[dict[str, Any]]:
        building_id = canonical_scope(building_id)
        matches: list[dict[str, Any]] = []
        for row in load_tauc_nycha_audit_rows():
            expected_prefix = canonical_scope(row.get("expected_prefix"))
            if expected_prefix != building_id:
                continue
            unit = parse_unit_token(row.get("expected_unit"))
            if not unit:
                continue
            actual_identity = canonical_identity(row.get("actual_identity"))
            actual_interface = str(row.get("actual_interface") or "").strip()
            if not actual_identity or not actual_interface:
                continue
            matches.append(
                {
                    "network_name": str(row.get("networkName") or "").strip(),
                    "unit": unit,
                    "classification": str(row.get("classification") or "").strip(),
                    "switch_identity": actual_identity,
                    "interface": actual_interface,
                    "mac": norm_mac(row.get("tauc_mac") or row.get("mac") or ""),
                    "evidence_sources": ["tauc_audit_exact_access_match"],
                }
            )
        return sorted(matches, key=lambda r: (r["unit"], r["switch_identity"], r["interface"]))

    def _nycha_inventory_rows_for_address(self, address: str) -> list[dict[str, str]]:
        target = normalize_address_text(address)
        rows: list[dict[str, str]] = []
        for row in load_nycha_info_rows():
            if normalize_address_text(row.get("Address")) != target:
                continue
            unit = parse_unit_token(row.get("Unit"))
            if not unit:
                continue
            rows.append(row)
        return rows

    def _direct_neighbor_edges(self, identity: str, ip: str) -> list[dict[str, Any]]:
        scan_id = self.latest_scan_id()
        rows = self.db.execute(
            "select interface, neighbor_identity, neighbor_address, platform, version from neighbors where scan_id=? and ip=? order by interface, neighbor_identity",
            (scan_id, ip),
        ).fetchall()
        edges: list[dict[str, Any]] = []
        for row in rows:
            interface = str(row["interface"] or "")
            if not is_direct_physical_interface(interface):
                continue
            neighbor_identity = canonical_identity(row["neighbor_identity"])
            if not neighbor_identity:
                continue
            edges.append(
                {
                    "from_identity": canonical_identity(identity),
                    "from_interface": interface.split(",", 1)[0],
                    "to_identity": neighbor_identity,
                    "neighbor_address": row["neighbor_address"],
                    "platform": row["platform"],
                    "version": row["version"],
                }
            )
        return edges

    def _address_inventory_online_unit_evidence(self, address: str) -> list[dict[str, Any]]:
        if not address:
            return []
        scan_id = self.latest_scan_id()
        nycha_rows = self._nycha_inventory_rows_for_address(address)
        if not nycha_rows:
            return []
        ppp_rows = [
            dict(r)
            for r in self.db.execute(
                """
                select p.router_ip, p.name, p.service, p.caller_id, p.address, p.uptime, d.identity
                from router_ppp_active p
                left join devices d on d.scan_id=p.scan_id and d.ip=p.router_ip
                where p.scan_id=?
                order by p.name
                """,
                (scan_id,),
            ).fetchall()
        ]
        ppp_by_name = {str(row.get("name") or "").strip(): row for row in ppp_rows if str(row.get("name") or "").strip()}
        arp_rows = [
            dict(r)
            for r in self.db.execute(
                "select router_ip, address, mac, interface, dynamic from router_arp where scan_id=?",
                (scan_id,),
            ).fetchall()
        ]
        arp_by_mac = {norm_mac(row.get("mac") or ""): row for row in arp_rows if norm_mac(row.get("mac") or "")}

        online_units: list[dict[str, Any]] = []
        for row in nycha_rows:
            unit = parse_unit_token(row.get("Unit"))
            if not unit:
                continue
            network_name = str(row.get("PPPoE") or "").strip()
            mac = norm_mac(row.get("MAC Address") or row.get("mac") or "")
            sources: list[str] = []
            if network_name and network_name in ppp_by_name:
                sources.append("router_pppoe_session")
            if mac and mac in arp_by_mac:
                sources.append("router_arp")
            if not sources:
                continue
            online_units.append(
                {
                    "unit": unit,
                    "network_name": network_name or None,
                    "mac": mac or None,
                    "sources": sorted(set(sources)),
                }
            )
        return online_units

    def get_building_model(self, building_id: str) -> dict[str, Any]:
        building_id = canonical_scope(building_id)
        if not building_id:
            raise ValueError("building_id is required")
        address_record = self._building_address_record(building_id) or {}
        building_health = self.get_building_health(building_id, include_alerts=False)
        customer_count = self.get_building_customer_count(building_id)
        exact_matches = self._exact_unit_port_matches(building_id)
        scan_id = self.latest_scan_id()
        nycha_rows = self._nycha_inventory_rows_for_address(str(address_record.get("address") or ""))
        nycha_by_unit: dict[str, dict[str, str]] = {}
        nycha_by_mac: dict[str, dict[str, str]] = {}
        for row in nycha_rows:
            unit = parse_unit_token(row.get("Unit"))
            if not unit:
                continue
            nycha_by_unit.setdefault(unit, row)
            mac = norm_mac(row.get("MAC Address") or row.get("mac") or "")
            if mac:
                nycha_by_mac[mac] = row

        live_bridge_hits = [
            dict(r)
            for r in self.db.execute(
                """
                select d.identity, bh.ip, bh.on_interface, bh.vid, bh.mac, bh.local, bh.external
                from bridge_hosts bh
                left join devices d on d.scan_id=bh.scan_id and d.ip=bh.ip
                where bh.scan_id=? and d.identity like ? and bh.local=0 and bh.on_interface like 'ether%'
                order by d.identity, bh.on_interface, bh.mac
                """,
                (scan_id, f"{building_id}%"),
            ).fetchall()
        ]
        live_bridge_hits = [row for row in live_bridge_hits if is_probable_customer_bridge_host(row)]
        bridge_hit_by_mac = {norm_mac(row.get("mac") or ""): row for row in live_bridge_hits if norm_mac(row.get("mac") or "")}

        exact_keys = {(row["unit"], row["switch_identity"], row["interface"]) for row in exact_matches}
        for mac, row in nycha_by_mac.items():
            hit = bridge_hit_by_mac.get(mac)
            unit = parse_unit_token(row.get("Unit"))
            if not hit or not unit:
                continue
            key = (unit, canonical_identity(hit.get("identity")), str(hit.get("on_interface") or "").strip())
            if key in exact_keys:
                continue
            exact_matches.append(
                {
                    "network_name": str(row.get("PPPoE") or "").strip(),
                    "unit": unit,
                    "classification": "nycha_info_mac_bridge_match",
                    "switch_identity": canonical_identity(hit.get("identity")),
                    "interface": str(hit.get("on_interface") or "").strip(),
                    "mac": mac,
                    "evidence_sources": ["nycha_info_mac", "bridge_host"],
                }
            )
            exact_keys.add(key)

        exact_matches = sorted(exact_matches, key=lambda r: (r["unit"], r["switch_identity"], r["interface"]))
        match_by_switch: dict[str, list[dict[str, Any]]] = {}
        for match in exact_matches:
            match_by_switch.setdefault(match["switch_identity"], []).append(match)

        devices = building_health.get("devices") or []
        switches: list[dict[str, Any]] = []
        direct_edges: list[dict[str, Any]] = []
        for device in devices:
            identity = canonical_identity(device.get("identity"))
            ip = str(device.get("ip") or "")
            if not identity or not ip:
                continue
            served_units = sorted({m["unit"] for m in match_by_switch.get(identity, [])})
            served_floors = sorted({int(re.match(r"(\d+)", unit).group(1)) for unit in served_units if re.match(r"(\d+)", unit)})
            edges = self._direct_neighbor_edges(identity, ip)
            direct_edges.extend(edges)
            switches.append(
                {
                    "identity": identity,
                    "ip": ip,
                    "model": device.get("model"),
                    "version": device.get("version"),
                    "served_units": served_units,
                    "served_floors": served_floors,
                    "exact_match_count": len(match_by_switch.get(identity, [])),
                    "direct_neighbors": edges,
                }
            )

        radios = [
            {
                "name": str(radio.get("name") or ""),
                "type": str(radio.get("type") or ""),
                "model": str(radio.get("model") or ""),
                "status": str(radio.get("status") or ""),
            }
            for radio in (self.get_site_topology(building_id.split(".")[0]).get("radios") or [])
            if canonical_scope(radio.get("resolved_building_id")) == building_id
        ]

        known_units = sorted(address_record.get("units") or [])
        exact_unit_set = {row["unit"] for row in exact_matches}
        ppp_rows = [
            dict(r)
            for r in self.db.execute(
                """
                select p.router_ip, p.name, p.service, p.caller_id, p.address, p.uptime, d.identity
                from router_ppp_active p
                left join devices d on d.scan_id=p.scan_id and d.ip=p.router_ip
                where p.scan_id=?
                order by p.name
                """,
                (scan_id,),
            ).fetchall()
        ]
        ppp_by_name = {str(row.get("name") or "").strip(): row for row in ppp_rows if str(row.get("name") or "").strip()}
        arp_rows = [
            dict(r)
            for r in self.db.execute(
                "select router_ip, address, mac, interface, dynamic from router_arp where scan_id=?",
                (scan_id,),
            ).fetchall()
        ]
        arp_by_mac = {norm_mac(row.get("mac") or ""): row for row in arp_rows if norm_mac(row.get("mac") or "")}
        unit_state_decisions: list[dict[str, Any]] = []
        exact_match_by_unit = {row["unit"]: row for row in exact_matches}
        for unit in known_units:
            inventory = nycha_by_unit.get(unit, {})
            network_name = str(inventory.get("PPPoE") or "").strip()
            mac = norm_mac(inventory.get("MAC Address") or inventory.get("mac") or "")
            sources: list[str] = []
            state = "unknown"
            exact = exact_match_by_unit.get(unit)
            if exact:
                state = "online"
                sources.extend(exact.get("evidence_sources") or ["bridge_host"])
            if network_name and network_name in ppp_by_name:
                state = "online"
                sources.append("router_pppoe_session")
            if mac and mac in arp_by_mac:
                state = "online"
                sources.append("router_arp")
            unit_state_decisions.append(
                {
                    "unit": unit,
                    "state": state,
                    "network_name": network_name or None,
                    "mac": mac or None,
                    "sources": sorted(set(sources)),
                    "switch_identity": exact.get("switch_identity") if exact else None,
                    "interface": exact.get("interface") if exact else None,
                }
            )

        live_port_pool = [
            {
                "switch_identity": canonical_identity(row.get("identity")),
                "interface": row.get("on_interface"),
                "mac": norm_mac(row.get("mac") or ""),
                "vid": row.get("vid"),
            }
            for row in (customer_count.get("results") or [])
        ]
        coverage = {
            "known_unit_count": len(known_units),
            "exact_unit_port_match_count": len(exact_matches),
            "exact_unit_port_coverage_pct": round((len(exact_unit_set) / len(known_units) * 100.0), 1) if known_units else 0.0,
            "live_port_pool_count": len(live_port_pool),
            "switch_count": len(switches),
            "direct_neighbor_edge_count": len(direct_edges),
        }
        return {
            "building_id": building_id,
            "site_id": building_id.split(".")[0],
            "address": address_record.get("address"),
            "known_units": known_units,
            "floors_inferred_from_units": max((int(re.match(r"(\d+)", unit).group(1)) for unit in known_units if re.match(r"(\d+)", unit)), default=0),
            "exact_unit_port_matches": exact_matches,
            "unit_state_decisions": unit_state_decisions,
            "live_port_pool": live_port_pool,
            "switches": switches,
            "direct_neighbor_edges": direct_edges,
            "radios": radios,
            "coverage": coverage,
            "data_gaps": {
                "building_geometry": "Jake has no authoritative facade/massing dataset for this building.",
                "full_unit_to_port_mapping": "Only TAUC-audited unit labels are exact today; the rest of the live ports are unmatched pool entries.",
                "switch_floor_placement": "Switch floor placement is only exact where unit-port matches exist; otherwise it remains inferred.",
            },
        }

    def get_vilo_server_info(self) -> dict[str, Any]:
        return self.vilo_api.summary() if self.vilo_api else {"configured": False}

    def get_vilo_inventory(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        return self.vilo_api.get_inventory(page_index, page_size)

    def audit_vilo_inventory(self, site_id: str | None = None, building_id: str | None = None, limit: int = 500) -> dict[str, Any]:
        return self.get_vilo_inventory_audit(site_id, building_id, limit)

    def search_vilo_inventory(self, filter_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        return self.vilo_api.search_inventory(filter_group or [], page_index, page_size)

    def get_vilo_subscribers(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        return self.vilo_api.get_subscribers(page_index, page_size)

    def search_vilo_subscribers(self, filter_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        return self.vilo_api.search_subscribers(filter_group or [], page_index, page_size)

    def get_vilo_networks(self, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        return self.vilo_api.get_networks(page_index, page_size)

    def search_vilo_networks(self, filter_group: list[dict[str, Any]] | None = None, sort_group: list[dict[str, Any]] | None = None, page_index: int = 1, page_size: int = 20) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        return self.vilo_api.search_networks(filter_group or [], sort_group or [], page_index, page_size)

    def get_vilo_devices(self, network_id: str) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        return self.vilo_api.get_devices(network_id)

    def search_vilo_devices(self, network_id: str, sort_group: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        return self.vilo_api.search_devices(network_id, sort_group or [])

    def get_tauc_network_name_list(self, status: str, page: int = 0, page_size: int = 100, name_prefix: str | None = None) -> dict[str, Any]:
        if not self.tauc:
            raise ValueError("TAUC cloud is not configured")
        return self.tauc.get_network_name_list(status, page, page_size, name_prefix)

    def get_tauc_network_details(self, network_id: str) -> dict[str, Any]:
        if not self.tauc:
            raise ValueError("TAUC cloud is not configured")
        return self.tauc.get_network_details(network_id)

    def get_tauc_preconfiguration_status(self, network_id: str) -> dict[str, Any]:
        if not self.tauc:
            raise ValueError("TAUC cloud is not configured")
        return self.tauc.get_preconfiguration_status(network_id)

    def get_tauc_pppoe_status(self, network_id: str, refresh: bool = True, include_credentials: bool = False) -> dict[str, Any]:
        if not self.tauc:
            raise ValueError("TAUC cloud is not configured")
        return self.tauc.get_pppoe_status(network_id, refresh, include_credentials)

    def get_tauc_device_id(self, sn: str, mac: str) -> dict[str, Any]:
        if not self.tauc:
            raise ValueError("TAUC cloud or ACS is not configured")
        return self.tauc.get_device_id(sn, mac)

    def get_tauc_device_detail(self, device_id: str) -> dict[str, Any]:
        if not self.tauc:
            raise ValueError("TAUC cloud or ACS is not configured")
        return self.tauc.get_device_detail(device_id)

    def get_tauc_device_internet(self, device_id: str) -> dict[str, Any]:
        if not self.tauc:
            raise ValueError("TAUC ACS is not configured")
        return self.tauc.get_device_internet(device_id)

    def get_tauc_olt_devices(self, mac: str | None, sn: str | None, status: str | None, page: int = 0, page_size: int = 50) -> dict[str, Any]:
        if not self.tauc:
            raise ValueError("TAUC OLT is not configured")
        return self.tauc.get_olt_devices(mac, sn, status, page, page_size)

    def get_building_customer_count(self, building_id: str) -> dict[str, Any]:
        building_id = canonical_scope(building_id)
        scan_id = self.latest_scan_id()
        address_record = self._building_address_record(building_id) or {}
        rows = self.db.execute(
            """
            select d.identity, bh.ip, bh.mac, bh.on_interface, bh.vid, bh.local, bh.external
            from bridge_hosts bh
            left join devices d on d.scan_id=bh.scan_id and d.ip=bh.ip
            where bh.scan_id=? and d.identity like ? and bh.local=0 and bh.on_interface like 'ether%'
            order by d.identity, bh.on_interface, bh.mac
            """,
            (scan_id, f"{building_id}%"),
        ).fetchall()
        results = [dict(r) for r in rows if is_probable_customer_bridge_host(dict(r))]
        evidence_online_units = self._address_inventory_online_unit_evidence(str(address_record.get("address") or ""))
        switches = sorted({r['identity'] for r in results if r.get('identity')})
        ports = sorted({(r['identity'], r['on_interface']) for r in results if r.get('identity') and r.get('on_interface')})
        vendor_summary = {'vilo': 0, 'tplink': 0, 'unknown': 0}
        for r in results:
            vendor_summary[mac_vendor_group(r['mac'])] += 1
        count = max(len(results), len(evidence_online_units))
        counting_method = 'bridge_hosts_external_access_ports'
        if len(evidence_online_units) > len(results):
            counting_method = 'max(bridge_hosts_external_access_ports, inventory_ppp_arp_unit_evidence)'
        return {
            'building_id': building_id,
            'scope_definition': f'all switches with identity prefix {building_id}.',
            'count': count,
            'counting_method': counting_method,
            'switch_count': len(switches),
            'switches': switches,
            'access_port_count': len(ports),
            'vendor_summary': vendor_summary,
            'evidence_backed_online_unit_count': len(evidence_online_units),
            'evidence_online_units': evidence_online_units[:500],
            'results': results[:500],
            'scan': self.latest_scan_meta(),
        }

    def _port_map_scope_rows(self, site_id: str | None = None, building_id: str | None = None) -> list[dict[str, Any]]:
        data = load_customer_port_map()
        rows = data.get("ports", [])
        if building_id:
            rows = [r for r in rows if identity_matches_scope(r.get("identity", ""), building_id)]
        elif site_id:
            rows = [r for r in rows if identity_matches_scope(r.get("identity", ""), site_id)]
        return [self._canonicalize_port_row(r) for r in rows]

    def _canonicalize_port_row(self, row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        ident = canonical_identity(out.get("identity"))
        out["identity"] = ident
        if ident:
            parts = ident.split(".")
            out["site_id"] = parts[0] if len(parts) >= 1 else None
            out["building_id"] = ".".join(parts[:2]) if len(parts) >= 2 else None
        else:
            out["site_id"] = None
            out["building_id"] = None
        return out

    def _fetch_vilo_inventory_rows(self, limit: int = 500, page_size: int = 50) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        requested_limit = max(1, int(limit))
        page_size = min(50, max(1, int(page_size)))
        rows: list[dict[str, Any]] = []
        total_count = 0
        pages_fetched = 0
        page_index = 1
        while len(rows) < requested_limit:
            payload = self.vilo_api.get_inventory(page_index, page_size)
            pages_fetched += 1
            data = payload.get("data") or {}
            batch = [dict(r) for r in (data.get("device_list") or [])]
            total_count = int(data.get("total_count") or total_count or len(batch))
            if not batch:
                break
            rows.extend(batch)
            if len(rows) >= total_count:
                break
            page_index += 1
        return {
            "rows": rows[:requested_limit],
            "inventory_total_count": total_count,
            "pages_fetched": pages_fetched,
            "limit_applied": total_count > requested_limit,
        }

    def _fetch_vilo_network_rows(self, limit: int = 2000, page_size: int = 50) -> list[dict[str, Any]]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        requested_limit = max(1, int(limit))
        page_size = min(50, max(1, int(page_size)))
        rows: list[dict[str, Any]] = []
        page_index = 1
        total_count = None
        while len(rows) < requested_limit:
            payload = self.vilo_api.get_networks(page_index, page_size)
            data = payload.get("data") or {}
            batch = [dict(r) for r in (data.get("network_list") or [])]
            if total_count is None:
                total_count = int(data.get("total_count") or len(batch))
            if not batch:
                break
            rows.extend(batch)
            if len(rows) >= (total_count or 0):
                break
            page_index += 1
        return rows[:requested_limit]

    def _fetch_vilo_subscriber_rows(self, limit: int = 500, page_size: int = 50) -> list[dict[str, Any]]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        requested_limit = max(1, int(limit))
        page_size = min(50, max(1, int(page_size)))
        rows: list[dict[str, Any]] = []
        page_index = 1
        total_count = None
        while len(rows) < requested_limit:
            payload = self.vilo_api.get_subscribers(page_index, page_size)
            data = payload.get("data") or {}
            batch = [dict(r) for r in (data.get("user_list") or [])]
            if total_count is None:
                total_count = int(data.get("total_count") or len(batch))
            if not batch:
                break
            rows.extend(batch)
            if len(rows) >= (total_count or 0):
                break
            page_index += 1
        return rows[:requested_limit]

    def _latest_vilo_scan_sightings(self, site_id: str | None = None, building_id: str | None = None) -> list[dict[str, Any]]:
        scan_id = self.latest_scan_id()
        raw_rows = [
            dict(r)
            for r in self.db.execute(
                """
                select d.identity, d.ip, bh.mac, bh.on_interface, bh.vid, bh.local, bh.external
                from bridge_hosts bh
                left join devices d on d.scan_id=bh.scan_id and d.ip=bh.ip
                where bh.scan_id=? and lower(bh.mac) like 'e8:da:00:%'
                order by d.identity, bh.on_interface, bh.mac
                """,
                (scan_id,),
            ).fetchall()
        ]
        port_rows = self._port_map_scope_rows(site_id=site_id, building_id=building_id)
        port_index = {(r.get("identity"), r.get("interface")): r for r in port_rows}
        out: list[dict[str, Any]] = []
        for row in raw_rows:
            identity = canonical_identity(row.get("identity"))
            if building_id and not identity_matches_scope(identity, building_id):
                continue
            if not building_id and site_id and not identity_matches_scope(identity, site_id):
                continue
            sighting = dict(row)
            sighting["identity"] = identity
            sighting["mac"] = norm_mac(sighting.get("mac") or "")
            parts = (identity or "").split(".")
            sighting["site_id"] = parts[0] if len(parts) >= 1 else None
            sighting["building_id"] = ".".join(parts[:2]) if len(parts) >= 2 else None
            port = port_index.get((identity, sighting.get("on_interface")))
            sighting["port_status"] = port.get("status") if port else None
            sighting["port_issues"] = (port.get("issues") or []) if port else []
            sighting["port_comment"] = (port.get("comment") or "") if port else ""
            out.append(sighting)
        return out

    def _derive_vilo_subscriber_hint(self, network: dict[str, Any] | None, sighting: dict[str, Any] | None) -> dict[str, Any] | None:
        sighting = sighting or {}
        network = network or {}
        comment = str(sighting.get("port_comment") or "").strip()
        network_name = str(network.get("network_name") or "").strip()
        building_id = str(sighting.get("building_id") or "").strip()
        if comment:
            return {
                "source": "port_comment",
                "label": comment,
                "building_id": building_id or None,
                "display": f"{building_id} {comment}".strip() if building_id else comment,
            }
        if network_name and not re.fullmatch(r"Vilo_[0-9a-fA-F]+", network_name):
            return {
                "source": "network_name",
                "label": network_name,
                "building_id": building_id or None,
                "display": network_name,
            }
        return None

    def _derive_building_from_network_name(self, network: dict[str, Any] | None) -> dict[str, Any] | None:
        network = network or {}
        network_name = str(network.get("network_name") or "").strip()
        if not network_name:
            return None
        explicit = re.search(r"\b(\d{6}\.\d{3})\b", network_name)
        if explicit:
            return {
                "source": "explicit_scope",
                "building_id": canonical_scope(explicit.group(1)),
                "label": network_name,
            }
        if re.fullmatch(r"Vilo_[0-9a-fA-F]+", network_name):
            return None
        resolved = self._resolve_building_from_address(network_name)
        best = resolved.get("best_match") or {}
        score = int(best.get("score") or 0)
        if best.get("prefix") and score >= 90:
            return {
                "source": "location_match",
                "building_id": canonical_scope(best.get("prefix")),
                "label": network_name,
                "score": score,
                "location": best.get("location"),
            }
        return None

    def get_vilo_inventory_audit(self, site_id: str | None = None, building_id: str | None = None, limit: int = 500) -> dict[str, Any]:
        if not self.vilo_api:
            raise ValueError("Vilo API is not configured")
        canonical_site_id = canonical_scope(site_id) if site_id else None
        canonical_building_id = canonical_scope(building_id) if building_id else None
        if canonical_site_id and canonical_building_id and not identity_matches_scope(canonical_building_id, canonical_site_id):
            raise ValueError("building_id must be within site_id when both are provided")

        inventory_info = self._fetch_vilo_inventory_rows(limit=max(2000, int(limit)), page_size=50)
        inventory_rows = inventory_info["rows"]
        inventory_by_mac: dict[str, dict[str, Any]] = {}
        for row in inventory_rows:
            mac = norm_mac(row.get("device_mac") or "")
            if mac and mac not in inventory_by_mac:
                inventory_by_mac[mac] = row
        network_rows = self._fetch_vilo_network_rows(limit=3000, page_size=50)
        subscriber_rows = self._fetch_vilo_subscriber_rows(limit=1000, page_size=50)
        networks_by_main_mac: dict[str, dict[str, Any]] = {}
        subscribers_by_id: dict[str, dict[str, Any]] = {}
        for row in network_rows:
            mac = norm_mac(row.get("main_vilo_mac") or "")
            if mac and mac not in networks_by_main_mac:
                networks_by_main_mac[mac] = row
        for row in subscriber_rows:
            subscriber_id = str(row.get("subscriber_id") or "").strip()
            if subscriber_id and subscriber_id not in subscribers_by_id:
                subscribers_by_id[subscriber_id] = row

        scan_rows = self._latest_vilo_scan_sightings(canonical_site_id, canonical_building_id)
        sightings_by_mac: dict[str, list[dict[str, Any]]] = {}
        for row in scan_rows:
            sightings_by_mac.setdefault(row["mac"], []).append(row)

        scope_active = bool(canonical_site_id or canonical_building_id)
        counts_by_classification: dict[str, int] = {}
        counts_by_inventory_status: dict[str, int] = {}
        counts_by_building: dict[str, int] = {}
        network_name_drift_count = 0
        rows: list[dict[str, Any]] = []
        live_only_rows: list[dict[str, Any]] = []

        def bump(counter: dict[str, int], key: str) -> None:
            counter[key] = counter.get(key, 0) + 1

        if scope_active:
            for mac, hits in sorted(sightings_by_mac.items()):
                best = best_bridge_hit(hits) or hits[0]
                inventory = inventory_by_mac.get(mac)
                network = networks_by_main_mac.get(mac)
                subscriber = subscribers_by_id.get(str((network or inventory or {}).get("subscriber_id") or "").strip())
                subscriber_hint = None if subscriber else self._derive_vilo_subscriber_hint(network, best)
                expected_building = self._derive_building_from_network_name(network)
                network_name_building_drift = bool(expected_building and best.get("building_id") and expected_building.get("building_id") != best.get("building_id"))
                classification = "inventory_matched" if inventory else "seen_not_in_vilo_inventory"
                if best.get("port_status") in {"isolated", "recovery_ready", "recovery_hold", "observe"}:
                    classification = f"{classification}_attention_port"
                row = {
                    "device_mac": mac,
                    "classification": classification,
                    "inventory_status": (inventory or {}).get("status"),
                    "device_sn": (inventory or {}).get("device_sn"),
                    "subscriber_id": (inventory or {}).get("subscriber_id"),
                    "network_id": (network or {}).get("network_id"),
                    "network_name": (network or {}).get("network_name"),
                    "network_status": (network or {}).get("network_status"),
                    "network_name_building_hint": expected_building,
                    "network_name_building_drift": network_name_building_drift,
                    "subscriber": {
                        "subscriber_id": (subscriber or {}).get("subscriber_id"),
                        "first_name": (subscriber or {}).get("first_name"),
                        "last_name": (subscriber or {}).get("last_name"),
                        "email": (subscriber or {}).get("email"),
                    } if subscriber else None,
                    "subscriber_hint": subscriber_hint,
                    "scan_seen": True,
                    "sighting": {
                        "identity": best.get("identity"),
                        "site_id": best.get("site_id"),
                        "building_id": best.get("building_id"),
                        "on_interface": best.get("on_interface"),
                        "vid": best.get("vid"),
                        "port_status": best.get("port_status"),
                        "port_issues": best.get("port_issues") or [],
                        "port_comment": best.get("port_comment") or "",
                    },
                }
                rows.append(row)
                bump(counts_by_classification, classification)
                bump(counts_by_building, best.get("building_id") or "unknown")
                if inventory and inventory.get("status"):
                    bump(counts_by_inventory_status, str(inventory.get("status")))
                if network_name_building_drift:
                    network_name_drift_count += 1
        else:
            for inventory in inventory_rows:
                mac = norm_mac(inventory.get("device_mac") or "")
                hits = sightings_by_mac.get(mac, [])
                best = best_bridge_hit(hits) if hits else None
                network = networks_by_main_mac.get(mac)
                subscriber = subscribers_by_id.get(str((network or inventory or {}).get("subscriber_id") or "").strip())
                subscriber_hint = None if subscriber else self._derive_vilo_subscriber_hint(network, best)
                expected_building = self._derive_building_from_network_name(network)
                network_name_building_drift = bool(expected_building and (best or {}).get("building_id") and expected_building.get("building_id") != (best or {}).get("building_id"))
                classification = "not_seen_in_latest_scan"
                if best:
                    classification = "seen_on_access_port" if is_edge_port(best.get("on_interface")) else "seen_on_non_access_port"
                    if best.get("port_status") in {"isolated", "recovery_ready", "recovery_hold", "observe"}:
                        classification = f"{classification}_attention_port"
                row = {
                    "device_mac": mac,
                    "classification": classification,
                    "inventory_status": inventory.get("status"),
                    "device_sn": inventory.get("device_sn"),
                    "subscriber_id": inventory.get("subscriber_id"),
                    "network_id": (network or {}).get("network_id"),
                    "network_name": (network or {}).get("network_name"),
                    "network_status": (network or {}).get("network_status"),
                    "network_name_building_hint": expected_building,
                    "network_name_building_drift": network_name_building_drift,
                    "subscriber": {
                        "subscriber_id": (subscriber or {}).get("subscriber_id"),
                        "first_name": (subscriber or {}).get("first_name"),
                        "last_name": (subscriber or {}).get("last_name"),
                        "email": (subscriber or {}).get("email"),
                    } if subscriber else None,
                    "subscriber_hint": subscriber_hint,
                    "scan_seen": bool(best),
                    "sighting": {
                        "identity": (best or {}).get("identity"),
                        "site_id": (best or {}).get("site_id"),
                        "building_id": (best or {}).get("building_id"),
                        "on_interface": (best or {}).get("on_interface"),
                        "vid": (best or {}).get("vid"),
                        "port_status": (best or {}).get("port_status"),
                        "port_issues": (best or {}).get("port_issues") or [],
                        "port_comment": (best or {}).get("port_comment") or "",
                    } if best else None,
                }
                rows.append(row)
                bump(counts_by_classification, classification)
                bump(counts_by_inventory_status, str(inventory.get("status") or "unknown"))
                bump(counts_by_building, ((best or {}).get("building_id") or "unresolved"))
                if network_name_building_drift:
                    network_name_drift_count += 1

            for mac, hits in sorted(sightings_by_mac.items()):
                if mac in inventory_by_mac:
                    continue
                best = best_bridge_hit(hits) or hits[0]
                network = networks_by_main_mac.get(mac)
                subscriber = subscribers_by_id.get(str((network or {}).get("subscriber_id") or "").strip())
                subscriber_hint = None if subscriber else self._derive_vilo_subscriber_hint(network, best)
                expected_building = self._derive_building_from_network_name(network)
                network_name_building_drift = bool(expected_building and best.get("building_id") and expected_building.get("building_id") != best.get("building_id"))
                live_only_rows.append(
                    {
                        "device_mac": mac,
                        "classification": "seen_not_in_vilo_inventory",
                        "network_id": (network or {}).get("network_id"),
                        "network_name": (network or {}).get("network_name"),
                        "network_name_building_hint": expected_building,
                        "network_name_building_drift": network_name_building_drift,
                        "subscriber": {
                            "subscriber_id": (subscriber or {}).get("subscriber_id"),
                            "first_name": (subscriber or {}).get("first_name"),
                            "last_name": (subscriber or {}).get("last_name"),
                            "email": (subscriber or {}).get("email"),
                        } if subscriber else None,
                        "subscriber_hint": subscriber_hint,
                        "sighting": {
                            "identity": best.get("identity"),
                            "site_id": best.get("site_id"),
                            "building_id": best.get("building_id"),
                            "on_interface": best.get("on_interface"),
                            "vid": best.get("vid"),
                            "port_status": best.get("port_status"),
                            "port_issues": best.get("port_issues") or [],
                            "port_comment": best.get("port_comment") or "",
                        },
                    }
                )

        return {
            "scan": self.latest_scan_meta(),
            "scope": {
                "site_id": canonical_site_id,
                "building_id": canonical_building_id,
                "scope_active": scope_active,
                "mode": "scope_scan_to_inventory" if scope_active else "inventory_to_scan",
            },
            "inventory_total_count": inventory_info["inventory_total_count"],
            "inventory_rows_examined": len(inventory_rows),
            "inventory_pages_fetched": inventory_info["pages_fetched"],
            "network_rows_examined": len(network_rows),
            "subscriber_rows_examined": len(subscriber_rows),
            "scope_seen_mac_count": len(sightings_by_mac),
            "subscriber_hint_count": sum(1 for row in rows if row.get("subscriber_hint")),
            "network_name_drift_count": network_name_drift_count,
            "counts_by_classification": dict(sorted(counts_by_classification.items())),
            "counts_by_inventory_status": dict(sorted(counts_by_inventory_status.items())),
            "counts_by_building": dict(sorted(counts_by_building.items())),
            "rows": rows,
            "live_only_rows": live_only_rows[:100],
            "limit_applied": inventory_info["limit_applied"],
        }

    def export_vilo_inventory_audit(self, site_id: str | None = None, building_id: str | None = None, limit: int = 500) -> dict[str, Any]:
        payload = self.get_vilo_inventory_audit(site_id, building_id, limit)
        scope = payload.get("scope") or {}
        scope_token = scope.get("building_id") or scope.get("site_id") or "global"
        safe_scope = str(scope_token).replace("/", "_")
        VILO_AUDIT_OUT_DIR.mkdir(parents=True, exist_ok=True)

        json_path = VILO_AUDIT_OUT_DIR / f"vilo_audit_{safe_scope}.json"
        csv_path = VILO_AUDIT_OUT_DIR / f"vilo_audit_{safe_scope}.csv"
        md_path = VILO_AUDIT_OUT_DIR / f"vilo_audit_{safe_scope}.md"

        latest_json = VILO_AUDIT_OUT_DIR / "vilo_audit_latest.json"
        latest_csv = VILO_AUDIT_OUT_DIR / "vilo_audit_latest.csv"
        latest_md = VILO_AUDIT_OUT_DIR / "vilo_audit_latest.md"

        json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        latest_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        rows = payload.get("rows") or []
        csv_rows: list[dict[str, Any]] = []
        for row in rows:
            sighting = row.get("sighting") or {}
            subscriber = row.get("subscriber") or {}
            subscriber_hint = row.get("subscriber_hint") or {}
            network_hint = row.get("network_name_building_hint") or {}
            csv_rows.append(
                {
                    "device_mac": row.get("device_mac"),
                    "classification": row.get("classification"),
                    "inventory_status": row.get("inventory_status"),
                    "device_sn": row.get("device_sn"),
                    "subscriber_id": row.get("subscriber_id"),
                    "network_id": row.get("network_id"),
                    "network_name": row.get("network_name"),
                    "network_status": row.get("network_status"),
                    "subscriber_first_name": subscriber.get("first_name"),
                    "subscriber_last_name": subscriber.get("last_name"),
                    "subscriber_email": subscriber.get("email"),
                    "subscriber_hint_source": subscriber_hint.get("source"),
                    "subscriber_hint_label": subscriber_hint.get("label"),
                    "subscriber_hint_display": subscriber_hint.get("display"),
                    "network_name_hint_source": network_hint.get("source"),
                    "expected_building_from_network_name": network_hint.get("building_id"),
                    "network_name_building_drift": row.get("network_name_building_drift"),
                    "site_id": sighting.get("site_id"),
                    "building_id": sighting.get("building_id"),
                    "identity": sighting.get("identity"),
                    "on_interface": sighting.get("on_interface"),
                    "vid": sighting.get("vid"),
                    "port_status": sighting.get("port_status"),
                    "port_issues": ",".join(sighting.get("port_issues") or []),
                    "port_comment": sighting.get("port_comment"),
                }
            )
        fieldnames = list(csv_rows[0].keys()) if csv_rows else [
            "device_mac", "classification", "inventory_status", "device_sn", "subscriber_id",
            "network_id", "network_name", "network_status", "subscriber_first_name",
            "subscriber_last_name", "subscriber_email", "subscriber_hint_source", "subscriber_hint_label", "subscriber_hint_display", "network_name_hint_source", "expected_building_from_network_name", "network_name_building_drift", "site_id", "building_id", "identity",
            "on_interface", "vid", "port_status", "port_issues", "port_comment",
        ]
        for target in (csv_path, latest_csv):
            with target.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_rows)

        counts = payload.get("counts_by_classification") or {}
        buildings = payload.get("counts_by_building") or {}
        matched_with_network = sum(1 for row in rows if row.get("network_id"))
        matched_with_subscriber = sum(1 for row in rows if row.get("subscriber"))
        matched_with_hint = sum(1 for row in rows if row.get("subscriber_hint"))
        network_name_drift = sum(1 for row in rows if row.get("network_name_building_drift"))
        attention = [r for r in rows if "attention_port" in str(r.get("classification") or "")]
        missing_inventory = [r for r in rows if str(r.get("classification") or "").startswith("seen_not_in_vilo_inventory")]
        drift_rows = [r for r in rows if r.get("network_name_building_drift")]

        lines = [
            "# Vilo Inventory Audit",
            "",
            "## Summary",
            "",
            f"- scope: `{scope_token}`",
            f"- scan id: `{(payload.get('scan') or {}).get('id')}`",
            f"- Vilo inventory total: `{payload.get('inventory_total_count', 0)}`",
            f"- inventory rows examined: `{payload.get('inventory_rows_examined', 0)}`",
            f"- live scan sightings in scope: `{payload.get('scope_seen_mac_count', 0)}`",
            f"- matched with Vilo network context: `{matched_with_network}`",
            f"- matched with Vilo subscriber context: `{matched_with_subscriber}`",
            f"- local fallback subscriber hints: `{matched_with_hint}`",
            f"- network-name building drift hits: `{network_name_drift}`",
            "",
            "## Classifications",
            "",
        ]
        if counts:
            for key, value in sorted(counts.items()):
                lines.append(f"- `{key}`: `{value}`")
        else:
            lines.append("- none")
        lines.extend([
            "",
            "## Buildings",
            "",
        ])
        if buildings:
            for key, value in sorted(buildings.items()):
                lines.append(f"- `{key}`: `{value}`")
        else:
            lines.append("- none")
        lines.extend([
            "",
            "## Network Name Building Drift",
            "",
        ])
        if drift_rows:
            for row in drift_rows[:25]:
                sighting = row.get("sighting") or {}
                hint = row.get("network_name_building_hint") or {}
                lines.append(
                    f"- `{row.get('device_mac')}` network `{row.get('network_name')}` implies `{hint.get('building_id')}` "
                    f"but is seen on `{sighting.get('building_id')}` `{sighting.get('identity')}` `{sighting.get('on_interface')}`"
                )
        else:
            lines.append("- none")
        lines.extend([
            "",
            "## Attention Ports",
            "",
        ])
        if attention:
            for row in attention[:25]:
                sighting = row.get("sighting") or {}
                lines.append(
                    f"- `{row.get('device_mac')}` `{row.get('network_name') or ''}` on "
                    f"`{sighting.get('identity')}` `{sighting.get('on_interface')}` "
                    f"status `{sighting.get('port_status')}` issues `{', '.join(sighting.get('port_issues') or []) or 'none'}`"
                )
        else:
            lines.append("- none")
        lines.extend([
            "",
            "## Seen In Scan But Missing From Vilo Inventory",
            "",
        ])
        if missing_inventory:
            for row in missing_inventory[:25]:
                sighting = row.get("sighting") or {}
                who = (row.get("subscriber") or {}).get("email") or row.get("network_name") or ""
                lines.append(
                    f"- `{row.get('device_mac')}` seen on `{sighting.get('identity')}` `{sighting.get('on_interface')}`"
                    + (f" linked to `{who}`" if who else "")
                )
        else:
            lines.append("- none")
        markdown = "\n".join(lines) + "\n"
        md_path.write_text(markdown, encoding="utf-8")
        latest_md.write_text(markdown, encoding="utf-8")

        return {
            "scope": scope,
            "paths": {
                "json": str(json_path),
                "csv": str(csv_path),
                "md": str(md_path),
                "latest_json": str(latest_json),
                "latest_csv": str(latest_csv),
                "latest_md": str(latest_md),
            },
            "summary": {
                "rows": len(rows),
                "matched_with_network": matched_with_network,
                "matched_with_subscriber": matched_with_subscriber,
                "matched_with_hint": matched_with_hint,
                "network_name_drift": network_name_drift,
                "counts_by_classification": counts,
            },
        }

    def get_building_flap_history(self, building_id: str) -> dict[str, Any]:
        rows = self._port_map_scope_rows(building_id=building_id)
        hits = [r for r in rows if "flap_history" in (r.get("issues") or [])]
        return {
            "building_id": building_id,
            "scope_definition": f"all switches with identity prefix {building_id}.",
            "count": len(hits),
            "ports": hits,
        }

    def get_site_flap_history(self, site_id: str) -> dict[str, Any]:
        rows = self._port_map_scope_rows(site_id=site_id)
        hits = [r for r in rows if "flap_history" in (r.get("issues") or [])]
        by_building: dict[str, int] = {}
        for row in hits:
            building_id = row.get("building_id") or "unknown"
            by_building[building_id] = by_building.get(building_id, 0) + 1
        return {
            "site_id": site_id,
            "count": len(hits),
            "building_count": len(by_building),
            "counts_by_building": dict(sorted(by_building.items())),
            "ports": hits,
        }

    def get_rogue_dhcp_suspects(self, building_id: str | None = None, site_id: str | None = None) -> dict[str, Any]:
        rows = self._port_map_scope_rows(site_id=site_id, building_id=building_id)
        hits = [
            r for r in rows
            if "rogue_dhcp_source_isolated" in (r.get("issues") or []) or "rogue_dhcp" in " ".join(r.get("issues") or [])
        ]
        return {
            "building_id": building_id,
            "site_id": site_id,
            "count": len(hits),
            "ports": hits,
        }

    def get_site_rogue_dhcp_summary(self, site_id: str) -> dict[str, Any]:
        rows = self._port_map_scope_rows(site_id=site_id)
        hits = [
            r for r in rows
            if "rogue_dhcp_source_isolated" in (r.get("issues") or []) or "rogue_dhcp" in " ".join(r.get("issues") or [])
        ]
        by_building: dict[str, dict[str, Any]] = {}
        for row in hits:
            identity = str(row.get("identity") or "")
            building_id = canonical_scope(".".join(identity.split(".")[:2]) if identity.count(".") >= 2 else identity)
            entry = by_building.setdefault(building_id, {"building_id": building_id, "count": 0, "isolated": 0, "ports": []})
            entry["count"] += 1
            if "rogue_dhcp_source_isolated" in (row.get("issues") or []):
                entry["isolated"] += 1
            entry["ports"].append(row)
        return {
            "site_id": site_id,
            "count": len(hits),
            "building_count": len(by_building),
            "buildings": sorted(by_building.values(), key=lambda x: x["building_id"]),
            "ports": hits,
        }

    def get_recovery_ready_cpes(self, building_id: str | None = None, site_id: str | None = None) -> dict[str, Any]:
        rows = self._port_map_scope_rows(site_id=site_id, building_id=building_id)
        hits = [r for r in rows if r.get("status") in {"recovery_ready", "recovery_hold"}]
        return {
            "building_id": building_id,
            "site_id": site_id,
            "count": len(hits),
            "ports": hits,
        }

    def get_site_punch_list(self, site_id: str) -> dict[str, Any]:
        rows = self._port_map_scope_rows(site_id=site_id)
        isolated = [r for r in rows if r.get("status") == "isolated"]
        recovery = [r for r in rows if r.get("status") in {"recovery_ready", "recovery_hold"}]
        flaps = [r for r in rows if "flap_history" in (r.get("issues") or [])]
        observe = [r for r in rows if r.get("status") == "observe"]
        actionable = [r for r in rows if r.get("status") in {"isolated", "recovery_ready", "recovery_hold", "observe"}]
        return {
            "site_id": site_id,
            "total_actionable_ports": len(actionable),
            "isolated_count": len(isolated),
            "recovery_count": len(recovery),
            "flap_count": len(flaps),
            "observe_count": len(observe),
            "isolated_ports": isolated,
            "recovery_ports": recovery,
            "flap_ports": flaps,
            "observe_ports": observe,
        }

    def get_switch_summary(self, switch_identity: str) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        device = self.db.execute(
            "select identity, ip, model, version from devices where scan_id=? and identity=? limit 1",
            (scan_id, switch_identity),
        ).fetchone()
        if not device:
            return {"switch_identity": switch_identity, "error": "No matching switch found in latest scan"}
        outliers = self.db.execute(
            "select interface, direction, severity, note from one_way_outliers o where o.scan_id=? and o.ip=? order by interface",
            (scan_id, device["ip"]),
        ).fetchall()
        hosts = self.db.execute(
            """
            select mac,on_interface,vid,local,external
            from bridge_hosts
            where scan_id=? and ip=?
            order by on_interface, mac
            """,
            (scan_id, device["ip"]),
        ).fetchall()
        probable_cpes = [
            dict(r)
            for r in hosts
            if r["on_interface"] and str(r["on_interface"]).startswith("ether") and bool(r["external"]) and not bool(r["local"])
        ]
        access_ports = sorted({r["on_interface"] for r in probable_cpes})
        vendor_summary = {"vilo": 0, "tplink": 0, "unknown": 0}
        for r in probable_cpes:
            vendor_summary[mac_vendor_group(r["mac"])] += 1
        return {
            "switch_identity": switch_identity,
            "scan": self.latest_scan_meta(),
            "device": dict(device),
            "outlier_count": len(outliers),
            "outliers": [dict(r) for r in outliers],
            "probable_cpe_count": len(probable_cpes),
            "access_port_count": len(access_ports),
            "access_ports": access_ports,
            "vendor_summary": vendor_summary,
            "probable_cpes": probable_cpes[:300],
        }

    def find_cpe_candidates(self, site_id: str | None, building_id: str | None, oui: str | None, access_only: bool, limit: int) -> dict[str, Any]:
        scan_id = self.latest_scan_id()
        prefix = building_id or site_id
        query = """
            select d.identity, bh.ip, bh.mac, bh.on_interface, bh.vid, bh.local, bh.external
            from bridge_hosts bh
            left join devices d on d.scan_id=bh.scan_id and d.ip=bh.ip
            where bh.scan_id=? and bh.external=1 and bh.local=0
        """
        params: list[Any] = [scan_id]
        if prefix:
            query += " and d.identity like ?"
            params.append(f"{prefix}%")
        if oui:
            norm_oui = norm_mac(oui + "000000")[:8]
            query += " and lower(bh.mac) like ?"
            params.append(f"{norm_oui.lower()}%")
        if access_only:
            query += " and bh.on_interface like 'ether%'"
        query += " order by d.identity, bh.on_interface limit ?"
        params.append(int(limit))
        rows = [dict(r) for r in self.db.execute(query, params)]
        return {
            "scan": self.latest_scan_meta(),
            "count": len(rows),
            "requested_limit": int(limit),
            "access_only": access_only,
            "results": rows,
            "limit_reached": len(rows) >= int(limit),
        }

    def get_cpe_state(self, mac: str, include_bigmac: bool) -> dict[str, Any]:
        mac = norm_mac(mac)
        scan_id = self.latest_scan_id()
        bridge = self.trace_mac(mac, include_bigmac)
        ppp = self.db.execute(
            "select router_ip, name, service, caller_id, address, uptime from router_ppp_active where scan_id=? and lower(caller_id)=lower(?) order by router_ip,name",
            (scan_id, mac),
        ).fetchall()
        arp = self.db.execute(
            "select router_ip, address, mac, interface, dynamic from router_arp where scan_id=? and lower(mac)=lower(?) order by router_ip,address",
            (scan_id, mac),
        ).fetchall()
        return {
            "mac": mac,
            "scan": self.latest_scan_meta(),
            "bridge": bridge,
            "ppp_sessions": [dict(r) for r in ppp],
            "arp_entries": [dict(r) for r in arp],
            "is_physically_seen": bool(bridge.get("verified_sightings")),
            "is_service_online": bool(ppp or arp),
        }


class MCPServer:
    def __init__(self) -> None:
        self.ops = JakeOps()

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
                result = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {"listChanged": False}}, "serverInfo": {"name": "jake-ops-mcp", "version": "0.1.0"}}
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
            self._write_message({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32000, "message": str(exc), "data": traceback.format_exc()}})

    def _call_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if name == "get_server_info":
            data = self.ops.get_server_info()
        elif name == "query_summary":
            data = self.ops.query_summary(arguments["query"])
        elif name == "get_outage_context":
            data = self.ops.get_outage_context(arguments["address_text"], arguments["unit"])
        elif name == "audit_device_labels":
            data = self.ops.audit_device_labels(bool(arguments.get("include_valid", False)), int(arguments.get("limit", 500)))
        elif name == "get_subnet_health":
            data = self.ops.get_subnet_health(arguments.get("subnet"), arguments.get("site_id"), bool(arguments.get("include_alerts", True)), bool(arguments.get("include_bigmac", True)))
        elif name == "get_online_customers":
            data = self.ops.get_online_customers(arguments.get("scope"), arguments.get("site_id"), arguments.get("building_id"), arguments.get("router_identity"))
        elif name == "trace_mac":
            data = self.ops.trace_mac(arguments["mac"], bool(arguments.get("include_bigmac", True)))
        elif name == "get_netbox_device":
            data = self.ops.get_netbox_device(arguments["name"])
        elif name == "get_site_alerts":
            data = self.ops.get_site_alerts(arguments["site_id"])
        elif name == "get_site_summary":
            data = self.ops.get_site_summary(arguments["site_id"], bool(arguments.get("include_alerts", True)))
        elif name == "get_site_topology":
            data = self.ops.get_site_topology(arguments["site_id"])
        elif name == "get_tauc_network_name_list":
            data = self.ops.get_tauc_network_name_list(arguments["status"], int(arguments.get("page", 0)), int(arguments.get("page_size", 100)), arguments.get("name_prefix"))
        elif name == "get_tauc_network_details":
            data = self.ops.get_tauc_network_details(arguments["network_id"])
        elif name == "get_tauc_preconfiguration_status":
            data = self.ops.get_tauc_preconfiguration_status(arguments["network_id"])
        elif name == "get_tauc_pppoe_status":
            data = self.ops.get_tauc_pppoe_status(arguments["network_id"], bool(arguments.get("refresh", True)), bool(arguments.get("include_credentials", False)))
        elif name == "get_tauc_device_id":
            data = self.ops.get_tauc_device_id(arguments["sn"], arguments["mac"])
        elif name == "get_tauc_device_detail":
            data = self.ops.get_tauc_device_detail(arguments["device_id"])
        elif name == "get_tauc_device_internet":
            data = self.ops.get_tauc_device_internet(arguments["device_id"])
        elif name == "get_tauc_olt_devices":
            data = self.ops.get_tauc_olt_devices(arguments.get("mac"), arguments.get("sn"), arguments.get("status"), int(arguments.get("page", 0)), int(arguments.get("page_size", 50)))
        elif name == "get_vilo_server_info":
            data = self.ops.get_vilo_server_info()
        elif name == "get_vilo_inventory":
            data = self.ops.get_vilo_inventory(int(arguments.get("page_index", 1)), int(arguments.get("page_size", 20)))
        elif name == "get_vilo_inventory_audit":
            data = self.ops.audit_vilo_inventory(arguments.get("site_id"), arguments.get("building_id"), int(arguments.get("limit", 500)))
        elif name == "export_vilo_inventory_audit":
            data = self.ops.export_vilo_inventory_audit(arguments.get("site_id"), arguments.get("building_id"), int(arguments.get("limit", 500)))
        elif name == "search_vilo_inventory":
            data = self.ops.search_vilo_inventory(arguments.get("filter") or [], int(arguments.get("page_index", 1)), int(arguments.get("page_size", 20)))
        elif name == "get_vilo_subscribers":
            data = self.ops.get_vilo_subscribers(int(arguments.get("page_index", 1)), int(arguments.get("page_size", 20)))
        elif name == "search_vilo_subscribers":
            data = self.ops.search_vilo_subscribers(arguments.get("filter") or [], int(arguments.get("page_index", 1)), int(arguments.get("page_size", 20)))
        elif name == "get_vilo_networks":
            data = self.ops.get_vilo_networks(int(arguments.get("page_index", 1)), int(arguments.get("page_size", 20)))
        elif name == "search_vilo_networks":
            data = self.ops.search_vilo_networks(arguments.get("filter") or [], arguments.get("sort") or [], int(arguments.get("page_index", 1)), int(arguments.get("page_size", 20)))
        elif name == "get_vilo_devices":
            data = self.ops.get_vilo_devices(arguments["network_id"])
        elif name == "search_vilo_devices":
            data = self.ops.search_vilo_devices(arguments["network_id"], arguments.get("sort_group") or [])
        elif name == "get_building_health":
            data = self.ops.get_building_health(arguments["building_id"], bool(arguments.get("include_alerts", True)))
        elif name == "get_building_model":
            data = self.ops.get_building_model(arguments["building_id"])
        elif name == "get_switch_summary":
            data = self.ops.get_switch_summary(arguments["switch_identity"])
        elif name == "get_building_customer_count":
            data = self.ops.get_building_customer_count(arguments["building_id"])
        elif name == "get_building_flap_history":
            data = self.ops.get_building_flap_history(arguments["building_id"])
        elif name == "get_site_flap_history":
            data = self.ops.get_site_flap_history(arguments["site_id"])
        elif name == "get_rogue_dhcp_suspects":
            data = self.ops.get_rogue_dhcp_suspects(arguments.get("building_id"), arguments.get("site_id"))
        elif name == "get_site_rogue_dhcp_summary":
            data = self.ops.get_site_rogue_dhcp_summary(arguments["site_id"])
        elif name == "get_recovery_ready_cpes":
            data = self.ops.get_recovery_ready_cpes(arguments.get("building_id"), arguments.get("site_id"))
        elif name == "get_site_punch_list":
            data = self.ops.get_site_punch_list(arguments["site_id"])
        elif name == "find_cpe_candidates":
            data = self.ops.find_cpe_candidates(arguments.get("site_id"), arguments.get("building_id"), arguments.get("oui"), bool(arguments.get("access_only", True)), int(arguments.get("limit", 100)))
        elif name == "get_cpe_state":
            data = self.ops.get_cpe_state(arguments["mac"], bool(arguments.get("include_bigmac", True)))
        else:
            raise ValueError(f"Unknown tool: {name}")
        return {"content": [{"type": "text", "text": json.dumps(data)}]}

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


if __name__ == "__main__":
    MCPServer().run()
