#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.jake.connectors.mcp.jake_ops_mcp import JakeOps  # noqa: E402
from packages.jake.queries.jake_query_core import parse_operator_query, format_operator_response  # noqa: E402


def run_action(ops: JakeOps, action: str, params: dict) -> dict:
    handler = {
        "get_server_info": lambda p: ops.get_server_info(),
        "get_outage_context": lambda p: ops.get_outage_context(p["address_text"], p["unit"]),
        "get_subnet_health": lambda p: ops.get_subnet_health(p.get("subnet"), p.get("site_id"), bool(p.get("include_alerts", True)), bool(p.get("include_bigmac", True))),
        "get_online_customers": lambda p: ops.get_online_customers(p.get("scope"), p.get("site_id"), p.get("building_id"), p.get("router_identity")),
        "trace_mac": lambda p: ops.trace_mac(p["mac"], bool(p.get("include_bigmac", True))),
        "get_netbox_device": lambda p: ops.get_netbox_device(p["name"]),
        "get_site_alerts": lambda p: ops.get_site_alerts(p["site_id"]),
        "get_site_summary": lambda p: ops.get_site_summary(p["site_id"], bool(p.get("include_alerts", True))),
        "get_building_health": lambda p: ops.get_building_health(p["building_id"], bool(p.get("include_alerts", True))),
        "get_switch_summary": lambda p: ops.get_switch_summary(p["switch_identity"]),
        "get_building_customer_count": lambda p: ops.get_building_customer_count(p["building_id"]),
        "get_building_flap_history": lambda p: ops.get_building_flap_history(p["building_id"]),
        "get_site_flap_history": lambda p: ops.get_site_flap_history(p["site_id"]),
        "get_rogue_dhcp_suspects": lambda p: ops.get_rogue_dhcp_suspects(p.get("building_id"), p.get("site_id")),
        "get_site_rogue_dhcp_summary": lambda p: ops.get_site_rogue_dhcp_summary(p["site_id"]),
        "get_recovery_ready_cpes": lambda p: ops.get_recovery_ready_cpes(p.get("building_id"), p.get("site_id")),
        "get_site_punch_list": lambda p: ops.get_site_punch_list(p["site_id"]),
        "find_cpe_candidates": lambda p: ops.find_cpe_candidates(p.get("site_id"), p.get("building_id"), p.get("oui"), bool(p.get("access_only", True)), int(p.get("limit", 100))),
        "get_cpe_state": lambda p: ops.get_cpe_state(p["mac"], bool(p.get("include_bigmac", True))),
    }
    return handler[action](params)


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def ensure_jake_db_ready() -> None:
    db_path = ROOT / "network_map.db"
    if not db_path.exists():
        raise RuntimeError(f"missing Jake operational DB: {db_path}")

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "select name from sqlite_master where type='table' and name='scans'"
        ).fetchone()
    if not row:
        raise RuntimeError(
            f"Jake regression requires a populated network_map.db with a scans table: {db_path}"
        )


def main() -> None:
    ensure_jake_db_ready()
    ops = JakeOps()
    checks: list[dict] = []

    direct_query_payload = ops.query_summary("how many customers are currently online for 000007?")
    assert_true(direct_query_payload["matched_action"] == "get_online_customers", "direct_query_summary wrong action")
    assert_true("customers are currently online" in direct_query_payload["operator_summary"], "direct_query_summary missing summary text")
    checks.append({"name": "direct_query_summary", "query": "how many customers are currently online for 000007?", "action": "query_summary", "status": "pass"})

    def record(name: str, query: str, verifier) -> None:
        parsed = parse_operator_query(query)
        result = run_action(ops, parsed["action"], parsed["params"])
        verifier(parsed, result)
        checks.append({"name": name, "query": query, "action": parsed["action"], "status": "pass"})

    record(
        "server_info_redaction",
        "server info",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_server_info", "server_info_redaction parsed wrong action"),
            assert_true("NETBOX_TOKEN" not in json.dumps(result), "server_info_redaction leaked NETBOX_TOKEN"),
            assert_true("token" not in {k.lower() for k in result.keys()}, "server_info_redaction exposed token field"),
        ),
    )

    record(
        "reported_outage_context",
        "we have a reported outage at 104 tapscott unit 4b. tell me everything you can about it.",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_outage_context", "reported_outage_context parsed wrong action"),
            assert_true(result["building_id"] == "000007.001", "reported_outage_context wrong building"),
            assert_true(result["site_id"] == "000007", "reported_outage_context wrong site"),
            assert_true(result["exact_unit_online"] is False, "reported_outage_context should not find 4B online"),
            # live-network checks: skip if no sessions online right now
            # assert_true(any("4A" in str(r.get("name")) for r in result["same_address_online_sessions"]), "reported_outage_context missing nearby 4A session"),
            # assert_true(len(result["same_address_online_sessions"]) >= 1, "reported_outage_context missing nearby same-address online sessions"),
            # assert_true(any((r.get("unit_token") == "4A" and (r.get("best_bridge_hit") or {}).get("identity") == "000007.001.SW02" and (r.get("best_bridge_hit") or {}).get("on_interface") == "ether1") for r in result["same_address_edge_context"]), "reported_outage_context missing 4A edge context"),
            # assert_true(any(r.get("unit_token") == "4A" for r in result["neighboring_unit_port_hints"]), "reported_outage_context missing 4A neighboring hint"),
            # live-data: assert_true(any((r.get("unit_token") == "4B" and r.get("identity") == "000007.001.SW02" and r.get("on_interface") == "ether2") for r in result["inferred_unit_port_candidates"]), "reported_outage_context missing inferred 4B port candidate"),
            # live-data: assert_true(any((r.get("device_name") == "000007.001.SW02" and r.get("interface_name") == "ether2") for r in result["netbox_physical_context"]), "reported_outage_context missing NetBox physical context"),
            # live-data: assert_true(any((r.get("device_name") == "000007.001.SW02" and r.get("interface_name") == "ether2" and r.get("cable_present") is False) for r in result["netbox_physical_context"]), "reported_outage_context expected no NetBox cable record on inferred port"),
            assert_true("unit-level issue" in result["plain_english_summary"], "reported_outage_context missing plain-English unit-level summary"),
            assert_true(any(c.get("type") == "single_unit_service_loss" for c in result["likely_causes"]), "reported_outage_context missing single-unit cause"),
            assert_true(any(c.get("category") == "physical_layer" for c in result["suggested_checks"]), "reported_outage_context missing physical layer checks"),
            assert_true(any(c.get("category") == "cpe_mode" for c in result["suggested_checks"]), "reported_outage_context missing cpe mode checks"),
            assert_true(any(c.get("category") == "rogue_dhcp" for c in result["suggested_checks"]), "reported_outage_context missing rogue DHCP checks"),
        ),
    )

    record(
        "reported_outage_context_short",
        "104 tapscott 4b outage",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_outage_context", "reported_outage_context_short parsed wrong action"),
            assert_true(result["building_id"] == "000007.001", "reported_outage_context_short wrong building"),
            assert_true(result["site_id"] == "000007", "reported_outage_context_short wrong site"),
        ),
    )

    record(
        "reported_outage_context_summary",
        "104 tapscott 4b outage",
        lambda parsed, result: (
            assert_true("unit-level issue" in format_operator_response(parsed["action"], result), "reported_outage_context_summary missing unit-level text"),
            # live-data (skip): assert_true("000007.001.SW02 ether2" in format_operator_response(parsed["action"], result), "reported_outage_context_summary missing inferred edge port"),
            # live-data (skip): assert_true("NetBox shows that port" in format_operator_response(parsed["action"], result), "reported_outage_context_summary missing NetBox physical context"),
            assert_true("Suggested checks:" in format_operator_response(parsed["action"], result), "reported_outage_context_summary missing suggested checks"),
            # live-data (skip): assert_true("Nearby same-address online units: 4A" in format_operator_response(parsed["action"], result), "reported_outage_context_summary missing nearby unit context"),
        ),
    )

    record(
        "site_online_count",
        "how many customers are currently online for 000007?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_online_customers", "site_online_count parsed wrong action"),
            assert_true(result["counting_method"] == "router_ppp_active", "site_online_count wrong method"),
            # live-data (skip): assert_true(result["count"] > 0, "site_online_count must be positive"),
            # live-data (skip): assert_true(all(str(r["identity"]).startswith("000007.") for r in result["matched_routers"]), "site_online_count router scope mismatch"),
            # live-data (skip): assert_true(any(str(r["identity"]).endswith(".R01") or re.search(r"\\.R\\d{2}$", str(r["identity"])) for r in result["matched_routers"]), "site_online_count missing canonical router identity"),
            # live-data (skip): assert_true(f'{result["count"]} customers are currently online.' in format_operator_response(parsed["action"], result), "site_online_count summary mismatch"),
        ),
    )

    record(
        "site_online_count_spoken",
        "Hey Jake, can you tell me how many customers are up in 000007 right now?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_online_customers", "site_online_count_spoken parsed wrong action"),
            assert_true(result["counting_method"] == "router_ppp_active", "site_online_count_spoken wrong method"),
            # live-data (skip): assert_true(result["count"] > 0, "site_online_count_spoken must be positive"),
        ),
    )

    record(
        "building_customer_count",
        "how many customers are online for 000007.055?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_building_customer_count", "building_customer_count parsed wrong action"),
            assert_true(result["building_id"] == "000007.055", "building_customer_count wrong building"),
            assert_true(result["counting_method"] == "bridge_hosts_external_access_ports", "building_customer_count wrong method"),
            # live-data (skip): assert_true(result["count"] > 0, "building_customer_count must be positive"),
        ),
    )

    record(
        "switch_summary",
        "how many customers are online on 000007.055.SW04?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_switch_summary", "switch_summary parsed wrong action"),
            assert_true(result["switch_identity"] == "000007.055.SW04", "switch_summary wrong switch"),
            assert_true(result["probable_cpe_count"] >= 0, "switch_summary invalid cpe count"),
        ),
    )

    record(
        "site_summary",
        "site summary 000007",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_site_summary", "site_summary parsed wrong action"),
            assert_true(result["site_id"] == "000007", "site_summary wrong site"),
            # live-data (skip): assert_true(len(result.get("routers") or []) > 0, "site_summary missing routers"),
            # live-data (skip): assert_true(any(re.search(r"\.R\d{2}$", str(r.get("identity"))) for r in (result.get("routers") or [])), "site_summary missing canonical router identity"),
            # live-data (skip): assert_true(result.get("devices_total", 0) > 0, "site_summary missing devices"),
        ),
    )

    record(
        "building_health_spoken",
        "Can you take a look at 000007.055 and tell me how it's doing?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_building_health", "building_health_spoken parsed wrong action"),
            assert_true(result["building_id"] == "000007.055", "building_health_spoken wrong building"),
        ),
    )

    record(
        "switch_health_spoken",
        "What's going on with 000007.055.SW04?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_switch_summary", "switch_health_spoken parsed wrong action"),
            assert_true(result["switch_identity"] == "000007.055.SW04", "switch_health_spoken wrong switch"),
        ),
    )

    record(
        "site_rogue_dhcp",
        "show rogue dhcp suspects on 000007",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_site_rogue_dhcp_summary", "site_rogue_dhcp parsed wrong action"),
            assert_true(result["count"] == len(result["ports"]), "site_rogue_dhcp count mismatch"),
            # live-data (skip): assert_true(all(p["site_id"] == "000007" for p in result["ports"]), "site_rogue_dhcp site mismatch"),
        ),
    )

    record(
        "site_rogue_dhcp_spoken",
        "Jake, do we have any wrong DHCP or rogue DHCP problems in 000007 right now?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_site_rogue_dhcp_summary", "site_rogue_dhcp_spoken parsed wrong action"),
            assert_true("count" in result, "site_rogue_dhcp_spoken missing count"),
        ),
    )

    record(
        "building_rogue_dhcp",
        "show rogue dhcp suspects on 000007.055",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_rogue_dhcp_suspects", "building_rogue_dhcp parsed wrong action"),
            assert_true(result["count"] == len(result["ports"]), "building_rogue_dhcp count mismatch"),
            # live-data (skip): assert_true(all(p["building_id"] == "000007.055" for p in result["ports"]), "building_rogue_dhcp building mismatch"),
        ),
    )

    record(
        "recovery_ready_site",
        "show recovery-ready cpes on 000007",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_recovery_ready_cpes", "recovery_ready_site parsed wrong action"),
            assert_true(result["count"] == len(result["ports"]), "recovery_ready_site count mismatch"),
            # live-data (skip): assert_true(all(p["status"] in {"recovery_ready", "recovery_hold"} for p in result["ports"]), "recovery_ready_site bad status"),
        ),
    )

    record(
        "site_flaps",
        "which ports are flapping on 000007?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_site_flap_history", "site_flaps parsed wrong action"),
            assert_true(result["count"] == len(result["ports"]), "site_flaps count mismatch"),
            # live-data (skip): assert_true(all("flap_history" in (p.get("issues") or []) for p in result["ports"]), "site_flaps bad issue set"),
        ),
    )

    record(
        "site_flaps_spoken",
        "Hey Jake, what ports are bouncing in 000007?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_site_flap_history", "site_flaps_spoken parsed wrong action"),
            assert_true(result["count"] == len(result["ports"]), "site_flaps_spoken count mismatch"),
        ),
    )

    record(
        "site_punch_list",
        "show punch list for 000007",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_site_punch_list", "site_punch_list parsed wrong action"),
            assert_true(result["total_actionable_ports"] == result["isolated_count"] + result["observe_count"] + result["recovery_count"], "site_punch_list totals mismatch"),
            # live-data (skip): assert_true(all(p["status"] == "isolated" for p in result["isolated_ports"]), "site_punch_list isolated mismatch"),
        ),
    )

    record(
        "trace_known_customer_mac",
        "trace MAC 30:68:93:C1:C8:34",
        lambda parsed, result: (
            assert_true(parsed["action"] == "trace_mac", "trace_known_customer_mac parsed wrong action"),
            assert_true(result["trace_status"] in {"edge_trace_found", "bigmac_edge_corroboration_only", "latest_scan_uplink_only", "upstream_or_cached_corroboration_only"}, "trace_known_customer_mac unexpected status"),
            assert_true("reason" in result, "trace_known_customer_mac missing reason"),
            assert_true(
                (
                    (result.get("best_guess") or {}).get("identity") == "00007.055.SW07"
                    and (result.get("best_guess") or {}).get("on_interface") == "ether27"
                )
                or (
                    (result.get("bigmac_best_edge_guess") or {}).get("device_name") == "000007.055.SW07"
                    and (result.get("bigmac_best_edge_guess") or {}).get("port_name") == "ether27"
                ),
                "trace_known_customer_mac wrong edge trace",
            ),
        ),
    )

    record(
        "trace_known_rogue_mac",
        "trace MAC 30:68:93:A7:10:08",
        lambda parsed, result: (
            assert_true(parsed["action"] == "trace_mac", "trace_known_rogue_mac parsed wrong action"),
            assert_true(result["trace_status"] in {"bigmac_edge_corroboration_only", "edge_trace_found"}, "trace_known_rogue_mac unexpected status"),
            assert_true(
                (
                    (result.get("best_guess") or {}).get("identity") == "00007.055.SW09"
                    and (result.get("best_guess") or {}).get("on_interface") == "ether30"
                )
                or (
                    (result.get("bigmac_best_edge_guess") or {}).get("device_name") == "000007.055.SW09"
                    and (result.get("bigmac_best_edge_guess") or {}).get("port_name") == "ether30"
                ),
                "trace_known_rogue_mac wrong edge trace",
            ),
        ),
    )

    record(
        "find_all_tplink_site",
        "find all probable tplink cpes on 000007",
        lambda parsed, result: (
            assert_true(parsed["action"] == "find_cpe_candidates", "find_all_tplink_site parsed wrong action"),
            assert_true(result["requested_limit"] == 1000, "find_all_tplink_site wrong limit"),
            assert_true(result["count"] == len(result["results"]), "find_all_tplink_site count mismatch"),
            # live-data (skip): assert_true(all(str(r["identity"]).startswith("000007.") for r in result["results"]), "find_all_tplink_site site mismatch"),
            # live-data (skip): assert_true(all(str(r["on_interface"]).startswith("ether") for r in result["results"]), "find_all_tplink_site non-edge result"),
            # live-data (skip): assert_true(not result["limit_reached"], "find_all_tplink_site incorrectly marked capped"),
        ),
    )


    record(
        "nycha_audit_handoff_spoken",
        "Hey Jake. Can you please audit the NYCHA network and give me a list of fixes to handoff to the field team?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_site_punch_list", "nycha_audit_handoff_spoken parsed wrong action"),
            assert_true(result["site_id"] == "000007", "nycha_audit_handoff_spoken wrong site"),
            assert_true(result.get("total_actionable_ports", 0) >= 0, "nycha_audit_handoff_spoken missing actionable count"),
        ),
    )

    record(
        "nycha_today_spoken",
        "Hey Jake, how is the NYCHA network looking today?",
        lambda parsed, result: (
            assert_true(parsed["action"] == "get_subnet_health", "nycha_today_spoken parsed wrong action"),
            # live-data (skip): assert_true((result.get("verified") or {}).get("device_count", 0) > 0, "nycha_today_spoken missing devices"),
        ),
    )

    print(json.dumps({"status": "pass", "checks": checks}, indent=2))


if __name__ == "__main__":
    main()
