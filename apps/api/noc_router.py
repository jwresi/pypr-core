from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from apps.api.jake_router import _ops
from packages.jake.connectors import slack as slack_connector
from packages.jake.graph.health import score_building, score_site
from packages.jake.graph.topology import get_graph
from packages.jake.incidents import store as incident_store

router = APIRouter(prefix="/v1/noc", tags=["noc"])


@router.get("/briefing", summary="Morning NOC briefing — full network state snapshot")
def briefing() -> dict:
    ops = _ops()
    graph = get_graph()
    now = datetime.now(timezone.utc).isoformat()

    try:
        summary = ops.get_site_summary("000007", include_alerts=True)
        devices_total = summary.get("devices_total", 0)
        switches_count = summary.get("switches_count", 0)
        active_alerts = summary.get("active_alerts") or []
    except Exception:
        devices_total = 0
        switches_count = 0
        active_alerts = []

    try:
        online = ops.get_online_customers(
            scope="000007",
            site_id="000007",
            building_id=None,
            router_identity=None,
        )
        customers_online = online.get("count", 0)
        counting_method = online.get("counting_method", "unknown")
    except Exception:
        customers_online = 0
        counting_method = "unavailable"

    try:
        dhcp = ops.get_site_rogue_dhcp_summary("000007")
        rogue_dhcp_count = dhcp.get("count", 0)
    except Exception:
        rogue_dhcp_count = 0

    try:
        flaps = ops.get_site_flap_history("000007")
        flap_count = flaps.get("count", 0)
        flap_ports = [port.get("port_identity") for port in (flaps.get("ports") or [])[:5]]
    except Exception:
        flap_count = 0
        flap_ports = []

    open_incidents = incident_store.list_incidents(status="investigating", limit=10)
    open_incidents += incident_store.list_incidents(status="identified", limit=10)

    graph_summary = graph.summary() if graph._built else {}
    spof_count = graph_summary.get("single_points_of_failure", 0)

    alert_names = []
    for alert in active_alerts[:10]:
        name = (alert.get("labels") or {}).get("alertname", "unknown")
        if name not in alert_names:
            alert_names.append(name)

    if active_alerts and flap_count > 0:
        network_status = "degraded"
    elif active_alerts or flap_count > 5 or rogue_dhcp_count > 0:
        network_status = "warning"
    else:
        network_status = "nominal"

    return {
        "generated_at": now,
        "site_id": "000007",
        "network_status": network_status,
        "customers": {
            "online": customers_online,
            "counting_method": counting_method,
        },
        "infrastructure": {
            "devices_total": devices_total,
            "switches": switches_count,
            "graph_nodes": graph_summary.get("total_nodes", 0),
            "single_points_of_failure": spof_count,
        },
        "alerts": {
            "count": len(active_alerts),
            "names": alert_names,
        },
        "flapping_ports": {
            "count": flap_count,
            "ports": flap_ports,
        },
        "rogue_dhcp": {
            "count": rogue_dhcp_count,
        },
        "open_incidents": {
            "count": len(open_incidents),
            "incidents": [
                {
                    "id": incident["incident_id"],
                    "scope": incident["scope"],
                    "severity": incident["severity"],
                    "started_at": incident["started_at"],
                    "signals": incident.get("signal_types", []),
                }
                for incident in open_incidents[:5]
            ],
        },
    }


@router.post("/briefing/post-to-slack", summary="Post current NOC briefing to Slack")
def post_briefing_to_slack() -> dict:
    current_briefing = briefing()
    ok = slack_connector.post_briefing(current_briefing)
    return {"posted": ok, "channel": slack_connector._channel()}


@router.get("/health-scores/{site_id}", summary="Risk scores for all buildings at a site")
def site_health_scores(site_id: str) -> dict:
    return score_site(site_id, _ops())


@router.get("/health-scores/{site_id}/{building_id}", summary="Risk score for a single building")
def building_health_score(site_id: str, building_id: str) -> dict:
    return score_building(building_id, _ops())
