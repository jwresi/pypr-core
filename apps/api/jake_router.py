from __future__ import annotations

from functools import lru_cache
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from packages.jake.connectors.mcp.jake_ops_mcp import JakeOps

router = APIRouter(prefix="/v1/jake", tags=["jake"])


@lru_cache(maxsize=1)
def _ops() -> JakeOps:
    return JakeOps()


# ── models ─────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    query: str


class QueryResponse(BaseModel):
    matched_action: str
    operator_summary: str
    result: Any


# ── operator query ─────────────────────────────────────────────────────────────

@router.post("/query", response_model=QueryResponse)
def operator_query(req: QueryRequest) -> QueryResponse:
    """Natural-language operator query — equivalent to MCP::jake_ops_mcp::query_summary."""
    try:
        raw = _ops().query_summary(req.query)
        return QueryResponse(
            matched_action=raw.get("matched_action", ""),
            operator_summary=raw.get("operator_summary", ""),
            result=raw.get("result"),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── server info ────────────────────────────────────────────────────────────────

@router.get("/info")
def server_info() -> dict:
    try:
        return _ops().get_server_info()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── site endpoints ─────────────────────────────────────────────────────────────

@router.get("/sites/{site_id}/summary")
def site_summary(site_id: str, include_alerts: bool = True) -> dict:
    try:
        return _ops().get_site_summary(site_id, include_alerts)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sites/{site_id}/topology")
def site_topology(site_id: str) -> dict:
    try:
        return _ops().get_site_topology(site_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sites/{site_id}/alerts")
def site_alerts(site_id: str) -> dict:
    try:
        return _ops().get_site_alerts(site_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sites/{site_id}/online-customers")
def online_customers(
    site_id: str,
    building_id: str | None = None,
    router_identity: str | None = None,
) -> dict:
    # scope can be "site_id" or "site_id.building_id"
    scope = f"{site_id}.{building_id}" if building_id else site_id
    try:
        return _ops().get_online_customers(
            scope=scope,
            site_id=site_id,
            building_id=building_id,
            router_identity=router_identity,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sites/{site_id}/flap-history")
def site_flap_history(site_id: str) -> dict:
    try:
        return _ops().get_site_flap_history(site_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sites/{site_id}/rogue-dhcp")
def site_rogue_dhcp(site_id: str) -> dict:
    try:
        return _ops().get_site_rogue_dhcp_summary(site_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── building endpoints ─────────────────────────────────────────────────────────

@router.get("/buildings/{building_id}/health")
def building_health(building_id: str, include_alerts: bool = True) -> dict:
    try:
        return _ops().get_building_health(building_id, include_alerts)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/buildings/{building_id}/model")
def building_model(building_id: str) -> dict:
    try:
        return _ops().get_building_model(building_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/buildings/{building_id}/customers")
def building_customers(building_id: str) -> dict:
    try:
        return _ops().get_building_customer_count(building_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/buildings/{building_id}/flap-history")
def building_flap_history(building_id: str) -> dict:
    try:
        return _ops().get_building_flap_history(building_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── switch ─────────────────────────────────────────────────────────────────────

@router.get("/switches/{switch_id}/summary")
def switch_summary(switch_id: str) -> dict:
    try:
        return _ops().get_switch_summary(switch_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── MAC / device ───────────────────────────────────────────────────────────────

@router.get("/mac")
def trace_mac(mac: str = Query(..., description="MAC address to trace"), include_bigmac: bool = True) -> dict:
    try:
        return _ops().trace_mac(mac, include_bigmac)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/netbox/devices/{name}")
def netbox_device(name: str) -> dict:
    try:
        return _ops().get_netbox_device(name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/recovery-ready-cpes")
def recovery_ready_cpes() -> dict:
    try:
        return _ops().get_recovery_ready_cpes()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── outage context ─────────────────────────────────────────────────────────────

@router.get("/outage-context")
def outage_context(
    address: str = Query(...),
    unit: str = Query(...),
) -> dict:
    try:
        return _ops().get_outage_context(address_text=address, unit=unit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── rogue DHCP ─────────────────────────────────────────────────────────────────

@router.get("/rogue-dhcp-suspects")
def rogue_dhcp_suspects() -> dict:
    try:
        return _ops().get_rogue_dhcp_suspects()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── subnet health ──────────────────────────────────────────────────────────────

@router.get("/subnets/health")
def subnet_health(
    subnet: str | None = None,
    site_id: str | None = None,
    include_alerts: bool = True,
    include_bigmac: bool = True,
) -> dict:
    try:
        return _ops().get_subnet_health(subnet, site_id, include_alerts, include_bigmac)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── TAUC ───────────────────────────────────────────────────────────────────────

@router.get("/tauc/networks")
def tauc_networks(
    status: str = "online",
    page: int = 0,
    page_size: int = 100,
    name_prefix: str | None = None,
) -> dict:
    try:
        return _ops().get_tauc_network_name_list(status, page, page_size, name_prefix)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/tauc/networks/{network_id}")
def tauc_network_details(network_id: str) -> dict:
    try:
        return _ops().get_tauc_network_details(network_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/tauc/devices/{device_id}")
def tauc_device_detail(device_id: str) -> dict:
    try:
        return _ops().get_tauc_device_detail(device_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/tauc/devices/{device_id}/internet")
def tauc_device_internet(device_id: str) -> dict:
    try:
        return _ops().get_tauc_device_internet(device_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/tauc/olt-devices")
def tauc_olt_devices(
    mac: str | None = None,
    sn: str | None = None,
    status: str | None = None,
    page: int = 0,
    page_size: int = 50,
) -> dict:
    try:
        return _ops().get_tauc_olt_devices(mac, sn, status, page, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Vilo ───────────────────────────────────────────────────────────────────────

@router.get("/vilo/info")
def vilo_info() -> dict:
    try:
        return _ops().get_vilo_server_info()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/vilo/inventory")
def vilo_inventory(page_index: int = 1, page_size: int = 20) -> dict:
    try:
        return _ops().get_vilo_inventory(page_index, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/vilo/inventory/search")
def vilo_inventory_search(q: str = Query(...)) -> dict:
    # search_vilo_inventory takes filter_group — wrap q as a simple name filter
    try:
        return _ops().search_vilo_inventory(
            filter_group=[{"field": "name", "operator": "contains", "value": q}]
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/vilo/subscribers")
def vilo_subscribers(page_index: int = 1, page_size: int = 20) -> dict:
    try:
        return _ops().get_vilo_subscribers(page_index, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/vilo/networks")
def vilo_networks(page_index: int = 1, page_size: int = 20) -> dict:
    try:
        return _ops().get_vilo_networks(page_index, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/vilo/devices")
def vilo_devices(network_id: str = Query(...)) -> dict:
    try:
        return _ops().get_vilo_devices(network_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
