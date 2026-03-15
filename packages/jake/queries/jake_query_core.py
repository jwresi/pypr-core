#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from typing import Any


def norm_scope(text: str) -> str:
    return text.strip().rstrip('?.!,')


def normalize_query(query: str) -> str:
    q = query.strip()
    q = re.sub(r'^\s*(hey|hi|hello)\s+jake[\s,:\-]*', '', q, flags=re.I)
    q = re.sub(r'^\s*jake[\s,:\-]*', '', q, flags=re.I)
    q = re.sub(r"\b(can you|could you|would you|please|i need you to|i need to know|tell me|show me|give me|let me know)\b", ' ', q, flags=re.I)
    q = re.sub(r"\b(what's going on with|whats going on with|what is going on with|look at|check on|check|take a look at|how is|how's)\b", ' ', q, flags=re.I)
    q = re.sub(r'\s+', ' ', q).strip()
    return q


def parse_operator_query(query: str) -> dict:
    q = normalize_query(query)
    lower = q.lower()

    mac_match = re.search(r'((?:[0-9a-f]{2}[:\-]){5}[0-9a-f]{2}|[0-9a-f]{12})', lower, re.I)
    subnet_match = re.search(r'(\d+\.\d+\.\d+\.\d+/\d+)', q)
    switch_match = re.search(r'\b(\d{6}\.\d{3}\.(?:SW\d+|RFSW\d+))\b', q)
    building_match = re.search(r'\b(\d{6}\.\d{3})\b', q)
    site_match = re.search(r'\b(\d{6})\b', q)
    outage_match = re.search(r'\b(?:reported\s+outage|outage|issue|problem)\s+at\s+(.+?)\s+(?:unit|apt|apartment)\s+([0-9a-z\-]+)\b', q, re.I)
    if not outage_match:
        outage_match = re.search(r'\bat\s+(.+?)\s+(?:unit|apt|apartment)\s+([0-9a-z\-]+)\b', q, re.I)
    if not outage_match and any(word in lower for word in ('outage', 'issue', 'problem', 'down')):
        outage_match = re.search(r'^\s*(\d+.*?\b[a-z]+(?:\s+[a-z]+)*)\s+([0-9]+[a-z])\s+(?:outage|issue|problem|down)\b', q, re.I)

    if outage_match:
        return {
            'action': 'get_outage_context',
            'params': {
                'address_text': norm_scope(outage_match.group(1)),
                'unit': norm_scope(outage_match.group(2)),
            },
        }
    if 'server info' in lower or 'server status' in lower:
        return {'action': 'get_server_info', 'params': {}}
    if 'vilo api' in lower and ('status' in lower or 'server info' in lower or 'configured' in lower):
        return {'action': 'get_vilo_server_info', 'params': {}}
    if 'vilo' in lower and any(token in lower for token in ('audit', 'reconcile', 'reconciliation')):
        params: dict[str, Any] = {'limit': 500}
        if building_match:
            params['building_id'] = building_match.group(1)
        elif site_match:
            params['site_id'] = site_match.group(1)
        if 'export' in lower or 'csv' in lower or 'markdown' in lower or 'report' in lower:
            return {'action': 'export_vilo_inventory_audit', 'params': params}
        return {'action': 'get_vilo_inventory_audit', 'params': params}
    if 'vilo inventory' in lower:
        return {'action': 'get_vilo_inventory', 'params': {'page_index': 1, 'page_size': 20}}
    if 'vilo subscribers' in lower:
        return {'action': 'get_vilo_subscribers', 'params': {'page_index': 1, 'page_size': 20}}
    if 'vilo networks' in lower:
        return {'action': 'get_vilo_networks', 'params': {'page_index': 1, 'page_size': 20}}
    if 'vilo devices' in lower or 'vilos for network' in lower:
        network_match = re.search(r'network(?:_id)?\s+([a-z0-9\-]{6,})', lower, re.I)
        if network_match:
            return {'action': 'get_vilo_devices', 'params': {'network_id': network_match.group(1)}}

    wants_audit = any(token in lower for token in ('audit', 'review', 'assess'))
    wants_handoff = any(token in lower for token in ('handoff', 'hand off', 'field team', 'fixes', 'action items', 'what needs to be fixed'))
    if (wants_audit or wants_handoff) and ('nycha' in lower or site_match or building_match):
        if building_match:
            return {'action': 'get_building_health', 'params': {'building_id': building_match.group(1), 'include_alerts': True}}
        if site_match:
            return {'action': 'get_site_punch_list', 'params': {'site_id': site_match.group(1)}}
        if 'nycha' in lower:
            return {'action': 'get_site_punch_list', 'params': {'site_id': '000007'}}

    if 'trace' in lower and mac_match:
        return {'action': 'trace_mac', 'params': {'mac': mac_match.group(1), 'include_bigmac': True}}
    if ('cpe state' in lower or 'device state' in lower or 'what is this device doing' in lower or 'what is this mac doing' in lower) and mac_match:
        return {'action': 'get_cpe_state', 'params': {'mac': mac_match.group(1), 'include_bigmac': True}}
    if ('site alerts' in lower or ('alerts' in lower and site_match)) and site_match:
        return {'action': 'get_site_alerts', 'params': {'site_id': site_match.group(1)}}
    if ('from netbox' in lower or 'netbox' in lower) and switch_match:
        return {'action': 'get_netbox_device', 'params': {'name': switch_match.group(1)}}
    if ('building health' in lower or ('how does' in lower and building_match) or ('how is' in lower and building_match) or ('status' in lower and building_match and not switch_match)) and building_match:
        return {'action': 'get_building_health', 'params': {'building_id': building_match.group(1), 'include_alerts': True}}
    if ('switch summary' in lower or ('how is' in lower and switch_match) or ('status' in lower and switch_match)) and switch_match:
        return {'action': 'get_switch_summary', 'params': {'switch_identity': switch_match.group(1)}}
    if ('site summary' in lower or ('how is' in lower and site_match and not building_match) or ('status' in lower and site_match and not building_match)) and site_match:
        return {'action': 'get_site_summary', 'params': {'site_id': site_match.group(1), 'include_alerts': True}}
    if 'nycha' in lower and any(token in lower for token in ('look today', 'look like today', 'today', 'status', 'health', 'how does', 'how is', 'looking', 'right now')):
        return {'action': 'get_subnet_health', 'params': {'subnet': '192.168.44.0/24', 'include_alerts': True, 'include_bigmac': False}}
    if 'odd behavior' in lower or 'health' in lower or ('look today' in lower and subnet_match) or ('looking today' in lower and ('nycha' in lower or subnet_match)):
        if subnet_match:
            return {'action': 'get_subnet_health', 'params': {'subnet': subnet_match.group(1), 'include_alerts': True, 'include_bigmac': False}}
        if switch_match:
            return {'action': 'get_switch_summary', 'params': {'switch_identity': switch_match.group(1)}}
        if building_match:
            return {'action': 'get_building_customer_count', 'params': {'building_id': building_match.group(1)}}
        if site_match:
            return {'action': 'get_site_summary', 'params': {'site_id': site_match.group(1), 'include_alerts': True}}
    if (('how many' in lower or 'count' in lower) and ('customer' in lower or 'subs' in lower or 'subscribers' in lower) and ('online' in lower or 'up' in lower or 'active' in lower)) or ('how many are up' in lower and (site_match or building_match or switch_match)):
        if switch_match:
            return {'action': 'get_switch_summary', 'params': {'switch_identity': switch_match.group(1)}}
        if building_match:
            return {'action': 'get_building_customer_count', 'params': {'building_id': building_match.group(1)}}
        if site_match:
            return {'action': 'get_online_customers', 'params': {'scope': site_match.group(1)}}
    if 'rogue dhcp' in lower or ('wrong dhcp' in lower) or ('bad dhcp' in lower) or ('dhcp server' in lower and ('rogue' in lower or 'wrong' in lower)):
        if building_match:
            return {'action': 'get_rogue_dhcp_suspects', 'params': {'building_id': building_match.group(1)}}
        if site_match:
            return {'action': 'get_site_rogue_dhcp_summary', 'params': {'site_id': site_match.group(1)}}
    if ('punch list' in lower or 'action items' in lower or 'what needs to be fixed' in lower) and site_match:
        return {'action': 'get_site_punch_list', 'params': {'site_id': site_match.group(1)}}
    if 'recovery ready' in lower or 'recovery-ready' in lower or 'ready for reboot' in lower or 'recovery hold' in lower or 'recovery-hold' in lower:
        if building_match:
            return {'action': 'get_recovery_ready_cpes', 'params': {'building_id': building_match.group(1)}}
        if site_match:
            return {'action': 'get_recovery_ready_cpes', 'params': {'site_id': site_match.group(1)}}
    if 'flap' in lower or 'flapping' in lower or 'bouncing' in lower or 'unstable ports' in lower:
        if building_match:
            return {'action': 'get_building_flap_history', 'params': {'building_id': building_match.group(1)}}
        if site_match:
            return {'action': 'get_site_flap_history', 'params': {'site_id': site_match.group(1)}}
    if (('find probable' in lower) or ('find all probable' in lower) or ('find cpe' in lower) or ('probable' in lower and ('tplink' in lower or 'vilo' in lower or 'cpe' in lower))) and ('tplink' in lower or 'vilo' in lower or 'cpe' in lower):
        oui = None
        if 'tplink' in lower:
            oui = '30:68:93'
        elif 'vilo' in lower:
            oui = 'E8:DA:00'
        limit = 100
        if ' all ' in f' {lower} ' or 'full' in lower or 'entire' in lower:
            limit = 1000
        elif building_match:
            limit = 300
        params = {'site_id': site_match.group(1) if site_match else None, 'building_id': building_match.group(1) if building_match else None, 'oui': oui, 'access_only': True, 'limit': limit}
        return {'action': 'find_cpe_candidates', 'params': params}
    generic_health_words = ('doing', 'look', 'looking', 'going on', 'status', 'today', 'right now')
    if building_match and not switch_match and any(word in lower for word in generic_health_words):
        return {'action': 'get_building_health', 'params': {'building_id': building_match.group(1), 'include_alerts': True}}
    if switch_match and any(word in lower for word in generic_health_words):
        return {'action': 'get_switch_summary', 'params': {'switch_identity': switch_match.group(1)}}
    if site_match and not building_match and any(word in lower for word in generic_health_words):
        return {'action': 'get_site_summary', 'params': {'site_id': site_match.group(1), 'include_alerts': True}}
    compact = re.sub(r'[^a-z0-9\.\s]', ' ', lower)
    compact = re.sub(r'\s+', ' ', compact).strip()
    if switch_match and compact == switch_match.group(1).lower():
        return {'action': 'get_switch_summary', 'params': {'switch_identity': switch_match.group(1)}}
    if building_match and compact == building_match.group(1).lower():
        return {'action': 'get_building_health', 'params': {'building_id': building_match.group(1), 'include_alerts': True}}
    if site_match and compact == site_match.group(1).lower():
        return {'action': 'get_site_summary', 'params': {'site_id': site_match.group(1), 'include_alerts': True}}
    raise ValueError('Could not map query to a deterministic Jake action')


def format_operator_response(action: str, result: dict, query: str | None = None) -> str:
    if action == 'get_outage_context':
        summary = result.get('plain_english_summary', 'No outage summary available.')
        summary = summary.split('Likely causes:')[0].strip()
        lines = [summary]
        inferred = result.get('inferred_unit_port_candidates') or []
        if inferred:
            top = inferred[0]
            lines.append(f"Most likely port: {top.get('identity')} {top.get('on_interface')} ({top.get('confidence')} confidence).")
        nearby = result.get('neighboring_unit_port_hints') or []
        if nearby:
            hints = ", ".join(f"{r.get('unit_token')} -> {(r.get('best_bridge_hit') or {}).get('identity')} {(r.get('best_bridge_hit') or {}).get('on_interface')}" for r in nearby[:3])
            lines.append(f"Nearby same-address online units: {hints}.")
        causes = result.get('likely_causes') or []
        if causes:
            lines.append("Likely causes:")
            lines.extend(f"- {c.get('reason')}" for c in causes[:4])
        checks = result.get('suggested_checks') or []
        if checks:
            lines.append("Suggested checks:")
            lines.extend(f"- {c.get('check')}" for c in checks[:5])
        alerts = result.get('active_alerts') or []
        if alerts:
            alert = alerts[0]
            labels = alert.get('labels', {})
            annotations = alert.get('annotations', {})
            lines.append(f"Active site alert present but separate from this unit: {labels.get('alertname')} - {annotations.get('summary') or labels.get('name')}.")
        return "\n".join(lines)

    if action == 'get_online_customers':
        count = result.get('count', 0)
        routers = ", ".join(f"{r.get('identity')} ({r.get('ip')})" for r in (result.get('matched_routers') or []))
        return f"{count} customers are currently online. Counting method: {result.get('counting_method')}. Routers used: {routers}."

    if action == 'get_subnet_health':
        verified = result.get('verified') or {}
        return (
            f"Latest scan {((verified.get('scan') or {}).get('id'))} saw "
            f"{verified.get('device_count', 0)} devices and "
            f"{verified.get('outlier_count', 0)} outliers."
        )

    if action == 'trace_mac':
        lines = [f"Trace status: {result.get('trace_status')}. {result.get('reason')}"]
        best = result.get('best_guess') or {}
        if best:
            lines.append(f"Best latest-scan sighting: {best.get('identity')} {best.get('on_interface')} VLAN {best.get('vid')}.")
        big = result.get('bigmac_best_edge_guess') or {}
        if big:
            lines.append(f"Best corroborated edge sighting: {big.get('device_name')} {big.get('port_name')} VLAN {big.get('vlan_id')}.")
        return "\n".join(lines)

    if action == 'get_site_summary':
        return (
            f"Site {result.get('site_id')} summary: {result.get('devices_total', 0)} devices, "
            f"{(result.get('online_customers') or {}).get('count', 0)} online customers, "
            f"{result.get('outlier_count', 0)} outliers, "
            f"{len(result.get('active_alerts') or [])} active alerts."
        )

    if action == 'get_vilo_inventory_audit':
        scope = result.get('scope') or {}
        scoped_to = scope.get('building_id') or scope.get('site_id') or 'global'
        matched_with_network = sum(1 for row in (result.get('rows') or []) if row.get('network_id'))
        matched_with_subscriber = sum(1 for row in (result.get('rows') or []) if row.get('subscriber'))
        matched_with_hint = sum(1 for row in (result.get('rows') or []) if row.get('subscriber_hint'))
        drift_count = sum(1 for row in (result.get('rows') or []) if row.get('network_name_building_drift'))
        return (
            f"Vilo audit {scoped_to}: {len(result.get('rows') or [])} rows analyzed, "
            f"{result.get('scope_seen_mac_count', 0)} scan sightings, "
            f"{matched_with_network} with network context, {matched_with_subscriber} with subscriber context, {matched_with_hint} with local hints, {drift_count} with network-name drift, "
            f"classifications={result.get('counts_by_classification') or {}}, "
            f"buildings={result.get('counts_by_building') or {}}."
        )

    if action == 'export_vilo_inventory_audit':
        paths = result.get('paths') or {}
        summary = result.get('summary') or {}
        return (
            f"Vilo audit export written. Rows={summary.get('rows', 0)}, "
            f"network_context={summary.get('matched_with_network', 0)}, "
            f"subscriber_context={summary.get('matched_with_subscriber', 0)}, "
            f"local_hints={summary.get('matched_with_hint', 0)}, "
            f"network_name_drift={summary.get('network_name_drift', 0)}. "
            f"CSV={paths.get('csv')} MD={paths.get('md')}."
        )

    if action == 'get_building_health':
        return (
            f"Building {result.get('building_id')} health: {result.get('device_count', 0)} devices, "
            f"{result.get('probable_cpe_count', 0)} probable CPEs, "
            f"{result.get('outlier_count', 0)} outliers, "
            f"{len(result.get('active_alerts') or [])} active alerts."
        )

    if action == 'get_switch_summary':
        vendor_summary = result.get('vendor_summary') or {}
        vendor_text = ", ".join(f"{k}={v}" for k, v in sorted(vendor_summary.items()) if v)
        return (
            f"Switch {result.get('switch_identity')} has {result.get('probable_cpe_count', 0)} probable CPEs "
            f"on {result.get('access_port_count', 0)} access ports. "
            f"Vendor mix: {vendor_text or 'none'}."
        )

    if action == 'get_site_punch_list':
        lines = [
            f"Site {result.get('site_id')} punch list: {result.get('total_actionable_ports', 0)} actionable ports, "
            f"{result.get('isolated_count', 0)} isolated, {result.get('recovery_count', 0)} recovery, "
            f"{result.get('observe_count', 0)} observe, {result.get('flap_count', 0)} with flap history."
        ]
        isolated = result.get('isolated_ports') or []
        recovery = result.get('recovery_ports') or []
        observe = result.get('observe_ports') or []
        if isolated:
            lines.append("Immediate isolate/investigate:")
            lines.extend(
                f"- {p.get('identity')} {p.get('port')} {('comment ' + p.get('comment')) if p.get('comment') else ''}".rstrip()
                for p in isolated[:5]
            )
        if recovery:
            lines.append("Recovery/reboot candidates:")
            lines.extend(
                f"- {p.get('identity')} {p.get('port')} {('comment ' + p.get('comment')) if p.get('comment') else ''}".rstrip()
                for p in recovery[:8]
            )
        if observe:
            lines.append("Field checks:")
            lines.extend(
                f"- {p.get('identity')} {p.get('port')} {('comment ' + p.get('comment')) if p.get('comment') else ''}".rstrip()
                for p in observe[:8]
            )
        return "\n".join(lines)

    if action == 'get_site_rogue_dhcp_summary':
        return f"Site {result.get('site_id')} has {result.get('count', 0)} rogue DHCP suspect ports across {result.get('building_count', 0)} buildings."

    if action == 'get_vilo_server_info':
        return (
            f"Vilo API configured={result.get('configured')} "
            f"base_url={result.get('base_url')} "
            f"token_cached={result.get('has_access_token')}."
        )

    if action == 'get_vilo_inventory':
        data = result.get('data') or {}
        return f"Vilo inventory returned {len(data.get('device_list') or [])} devices out of total_count {data.get('total_count', 0)}."

    if action == 'get_vilo_subscribers':
        data = result.get('data') or {}
        return f"Vilo subscribers returned {len(data.get('subscriber_list') or [])} subscribers out of total_count {data.get('total_count', 0)}."

    if action == 'get_vilo_networks':
        data = result.get('data') or {}
        return f"Vilo networks returned {len(data.get('network_list') or [])} networks out of total_count {data.get('total_count', 0)}."

    if action == 'get_vilo_devices':
        data = result.get('data') or {}
        return f"Vilo device detail returned {len(data.get('vilo_info_list') or [])} devices for network {query or ''}".strip()

    return json.dumps(result, indent=2)


def run_operator_query(ops: Any, query: str) -> dict[str, Any]:
    parsed = parse_operator_query(query)
    handler = {
        'get_server_info': lambda p: ops.get_server_info(),
        'get_outage_context': lambda p: ops.get_outage_context(p['address_text'], p['unit']),
        'get_subnet_health': lambda p: ops.get_subnet_health(p.get('subnet'), p.get('site_id'), bool(p.get('include_alerts', True)), bool(p.get('include_bigmac', True))),
        'get_online_customers': lambda p: ops.get_online_customers(p.get('scope'), p.get('site_id'), p.get('building_id'), p.get('router_identity')),
        'trace_mac': lambda p: ops.trace_mac(p['mac'], bool(p.get('include_bigmac', True))),
        'get_netbox_device': lambda p: ops.get_netbox_device(p['name']),
        'get_site_alerts': lambda p: ops.get_site_alerts(p['site_id']),
        'get_site_summary': lambda p: ops.get_site_summary(p['site_id'], bool(p.get('include_alerts', True))),
        'get_building_health': lambda p: ops.get_building_health(p['building_id'], bool(p.get('include_alerts', True))),
        'get_switch_summary': lambda p: ops.get_switch_summary(p['switch_identity']),
        'get_building_customer_count': lambda p: ops.get_building_customer_count(p['building_id']),
        'get_building_flap_history': lambda p: ops.get_building_flap_history(p['building_id']),
        'get_site_flap_history': lambda p: ops.get_site_flap_history(p['site_id']),
        'get_rogue_dhcp_suspects': lambda p: ops.get_rogue_dhcp_suspects(p.get('building_id'), p.get('site_id')),
        'get_site_rogue_dhcp_summary': lambda p: ops.get_site_rogue_dhcp_summary(p['site_id']),
        'get_recovery_ready_cpes': lambda p: ops.get_recovery_ready_cpes(p.get('building_id'), p.get('site_id')),
        'get_site_punch_list': lambda p: ops.get_site_punch_list(p['site_id']),
        'find_cpe_candidates': lambda p: ops.find_cpe_candidates(p.get('site_id'), p.get('building_id'), p.get('oui'), bool(p.get('access_only', True)), int(p.get('limit', 100))),
        'get_cpe_state': lambda p: ops.get_cpe_state(p['mac'], bool(p.get('include_bigmac', True))),
        'get_vilo_server_info': lambda p: ops.get_vilo_server_info(),
        'get_vilo_inventory': lambda p: ops.get_vilo_inventory(int(p.get('page_index', 1)), int(p.get('page_size', 20))),
        'get_vilo_inventory_audit': lambda p: ops.audit_vilo_inventory(p.get('site_id'), p.get('building_id'), int(p.get('limit', 500))),
        'export_vilo_inventory_audit': lambda p: ops.export_vilo_inventory_audit(p.get('site_id'), p.get('building_id'), int(p.get('limit', 500))),
        'get_vilo_subscribers': lambda p: ops.get_vilo_subscribers(int(p.get('page_index', 1)), int(p.get('page_size', 20))),
        'get_vilo_networks': lambda p: ops.get_vilo_networks(int(p.get('page_index', 1)), int(p.get('page_size', 20))),
        'get_vilo_devices': lambda p: ops.get_vilo_devices(p['network_id']),
    }
    result = handler[parsed['action']](parsed['params'])
    return {
        'query': query,
        'matched_action': parsed['action'],
        'params': parsed['params'],
        'operator_summary': format_operator_response(parsed['action'], result, query),
        'result': result,
    }
