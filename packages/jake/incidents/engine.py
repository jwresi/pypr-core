from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from packages.jake.graph.topology import get_graph
from packages.jake.incidents import store as incident_store


class Severity(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class IncidentStatus(str, Enum):
    investigating = "investigating"
    identified = "identified"
    monitoring = "monitoring"
    resolved = "resolved"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_incident(scope: str, signals: list[dict], ops: Any) -> dict:
    incident_id = f"INC-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{scope}"
    graph = get_graph()

    blast = graph.blast_radius(scope) if graph._built else {}
    affected_buildings = blast.get("affected_buildings", [])
    affected_switches = blast.get("affected_switches", [])

    signal_types = list({signal.get("type") for signal in signals if signal.get("type")})
    has_flaps = any(signal.get("type") == "port_flap" for signal in signals)
    has_alert = any(signal.get("type") == "alert" for signal in signals)
    has_customer_drop = any(signal.get("type") == "customer_count_drop" for signal in signals)
    has_rogue_dhcp = any(signal.get("type") == "rogue_dhcp" for signal in signals)

    if has_customer_drop and has_alert:
        severity = Severity.critical
    elif has_customer_drop or has_alert:
        severity = Severity.high
    elif has_flaps or has_rogue_dhcp:
        severity = Severity.medium
    else:
        severity = Severity.low

    causes = []
    if has_flaps and has_customer_drop:
        causes.append({"type": "uplink_instability", "confidence": 0.8})
    if has_rogue_dhcp:
        causes.append({"type": "rogue_dhcp_server", "confidence": 0.9})
    if has_alert and not has_flaps:
        causes.append({"type": "external_alert", "confidence": 0.6})
    if not causes:
        causes.append({"type": "unknown", "confidence": 0.3})

    actions = []
    if has_flaps:
        actions.append({"action": "check_uplink_optics", "priority": 1})
        actions.append({"action": "review_interface_errors", "priority": 2})
    if has_rogue_dhcp:
        actions.append({"action": "isolate_rogue_dhcp_port", "priority": 1})
    if has_customer_drop:
        actions.append({"action": "verify_pppoe_sessions", "priority": 2})
    if has_alert:
        actions.append({"action": "acknowledge_alertmanager_alert", "priority": 3})

    incident = {
        "incident_id": incident_id,
        "scope": scope,
        "severity": severity.value,
        "status": IncidentStatus.investigating.value,
        "started_at": _now(),
        "signals": signals,
        "signal_types": signal_types,
        "blast_radius": {
            "affected_buildings": affected_buildings,
            "affected_switches": affected_switches,
            "total_downstream": blast.get("total_downstream", 0),
        },
        "probable_causes": causes,
        "recommended_actions": actions,
        "notes": [],
    }
    incident_store.save_incident(incident)
    return incident


def correlate_from_jake(scope: str, ops: Any) -> dict:
    signals = []

    try:
        if "." in scope and scope.count(".") == 1:
            flaps = ops.get_building_flap_history(scope)
        else:
            flaps = ops.get_site_flap_history(scope)
        if flaps.get("count", 0) > 0:
            signals.append(
                {
                    "type": "port_flap",
                    "count": flaps["count"],
                    "ports": [port.get("port_identity") for port in flaps.get("ports", [])[:5]],
                }
            )
    except Exception:
        pass

    try:
        site_id = scope.split(".")[0]
        alerts = ops.get_site_alerts(site_id)
        active = alerts.get("alerts") or alerts.get("active_alerts") or []
        if active:
            signals.append(
                {
                    "type": "alert",
                    "count": len(active),
                    "names": [alert.get("labels", {}).get("alertname") for alert in active[:3]],
                }
            )
    except Exception:
        pass

    try:
        if "." in scope:
            dhcp = ops.get_rogue_dhcp_suspects(building_id=scope)
            building_ports = dhcp.get("ports") or []
        else:
            dhcp = ops.get_site_rogue_dhcp_summary(scope)
            building_ports = dhcp.get("ports") or []
        if building_ports:
            signals.append(
                {
                    "type": "rogue_dhcp",
                    "count": len(building_ports),
                    "ports": [port.get("port_identity") for port in building_ports[:3]],
                }
            )
    except Exception:
        pass

    return create_incident(scope, signals, ops)
