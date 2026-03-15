from __future__ import annotations

from typing import Any

from packages.jake.graph.topology import get_graph


def score_building(building_id: str, ops: Any) -> dict:
    graph = get_graph()
    score = 100
    factors = []

    try:
        flaps = ops.get_building_flap_history(building_id)
        flap_count = flaps.get("count", 0)
        if flap_count > 0:
            penalty = min(40, flap_count * 10)
            score -= penalty
            factors.append({"factor": "port_flaps", "count": flap_count, "penalty": -penalty})
    except Exception:
        pass

    try:
        dhcp = ops.get_rogue_dhcp_suspects(building_id=building_id)
        building_ports = dhcp.get("ports") or []
        if building_ports:
            score -= 30
            factors.append({"factor": "rogue_dhcp", "count": len(building_ports), "penalty": -30})
    except Exception:
        pass

    if graph._built:
        switches_in_building = [
            node
            for node in graph.g.nodes
            if graph.g.nodes[node].get("building_id") == building_id
            and graph.g.nodes[node].get("type") == "switch"
        ]
        spof_switches = [
            switch
            for switch in switches_in_building
            if graph.redundancy_check(switch)["is_single_point_of_failure"]
        ]
        if spof_switches:
            penalty = min(30, len(spof_switches) * 15)
            score -= penalty
            factors.append(
                {
                    "factor": "spof_switches",
                    "count": len(spof_switches),
                    "penalty": -penalty,
                    "switches": spof_switches[:3],
                }
            )

    score = max(0, score)

    if score >= 80:
        risk = "low"
    elif score >= 50:
        risk = "medium"
    elif score >= 25:
        risk = "high"
    else:
        risk = "critical"

    return {
        "building_id": building_id,
        "score": score,
        "risk": risk,
        "factors": factors,
    }


def score_site(site_id: str, ops: Any, buildings: list[str] | None = None) -> dict:
    graph = get_graph()

    if buildings is None and graph._built:
        buildings = list(
            {
                graph.g.nodes[node].get("identity")
                for node in graph.g.nodes
                if graph.g.nodes[node].get("type") == "building"
                and graph.g.nodes[node].get("site_id") == site_id
            }
            - {None}
        )

    if not buildings:
        return {"site_id": site_id, "buildings_scored": 0, "scores": []}

    scores = []
    for building_id in buildings:
        try:
            scores.append(score_building(building_id, ops))
        except Exception:
            pass

    scores.sort(key=lambda item: item["score"])

    return {
        "site_id": site_id,
        "buildings_scored": len(scores),
        "critical": [item for item in scores if item["risk"] == "critical"],
        "high": [item for item in scores if item["risk"] == "high"],
        "medium": [item for item in scores if item["risk"] == "medium"],
        "low": [item for item in scores if item["risk"] == "low"],
        "scores": scores,
    }
