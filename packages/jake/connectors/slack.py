from __future__ import annotations

import json
import os
import urllib.request


def _webhook_url() -> str:
    return os.environ.get("SLACK_WEBHOOK_URL", "")


def _channel() -> str:
    return os.environ.get("SLACK_NOC_CHANNEL", "#jake")


def _post_webhook(payload: dict) -> bool:
    url = _webhook_url()
    if not url:
        return False
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False


def _status_emoji(s: str) -> str:
    return {
        "nominal": ":large_green_circle:",
        "warning": ":large_yellow_circle:",
        "degraded": ":red_circle:",
    }.get(s, ":white_circle:")


def _sev_emoji(s: str) -> str:
    return {
        "critical": ":rotating_light:",
        "high": ":red_circle:",
        "medium": ":large_yellow_circle:",
        "low": ":white_circle:",
    }.get(s, ":white_circle:")


def post_briefing(briefing: dict) -> bool:
    status = briefing.get("network_status", "unknown")
    customers = briefing.get("customers", {}).get("online", 0)
    alerts = briefing.get("alerts", {})
    flaps = briefing.get("flapping_ports", {})
    rogue = briefing.get("rogue_dhcp", {})
    infra = briefing.get("infrastructure", {})
    incidents = briefing.get("open_incidents", {})
    alert_names = ", ".join(alerts.get("names", [])) or "none"
    ts = briefing.get("generated_at", "")[:16]

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"NOC Briefing — site 000007 — {ts}Z",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"{_status_emoji(status)} *Status*\n{status.upper()}"},
                {"type": "mrkdwn", "text": f":busts_in_silhouette: *Customers online*\n{customers}"},
                {"type": "mrkdwn", "text": f":bell: *Active alerts*\n{alerts.get('count', 0)} — {alert_names}"},
                {"type": "mrkdwn", "text": f":arrows_counterclockwise: *Flapping ports*\n{flaps.get('count', 0)}"},
                {"type": "mrkdwn", "text": f":warning: *Rogue DHCP*\n{rogue.get('count', 0)}"},
                {
                    "type": "mrkdwn",
                    "text": f":building_construction: *SPOFs*\n{infra.get('single_points_of_failure', 0)}",
                },
            ],
        },
    ]

    if incidents.get("count", 0) > 0:
        inc_lines = "\n".join(
            f"{_sev_emoji(i['severity'])} `{i['scope']}` — {i['severity']} — {', '.join(i.get('signals', []))}"
            for i in incidents.get("incidents", [])
        )
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":rotating_light: *Open incidents ({incidents['count']})*\n{inc_lines}",
                },
            }
        )

    return _post_webhook(
        {
            "channel": _channel(),
            "blocks": blocks,
            "text": f"NOC Briefing — {status.upper()} — {alerts.get('count', 0)} alerts",
        }
    )


def post_incident_alert(incident: dict) -> bool:
    scope = incident.get("scope", "unknown")
    severity = incident.get("severity", "unknown")
    signals = ", ".join(incident.get("signal_types", [])) or "unknown"
    blast = incident.get("blast_radius", {})
    buildings = len(blast.get("affected_buildings", []))
    causes = ", ".join(c["type"] for c in incident.get("probable_causes", []))
    actions = ", ".join(a["action"] for a in incident.get("recommended_actions", []))

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{_sev_emoji(severity)} Incident — {scope}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Severity*\n{severity.upper()}"},
                {"type": "mrkdwn", "text": f"*Signals*\n{signals}"},
                {"type": "mrkdwn", "text": f"*Blast radius*\n{buildings} buildings affected"},
                {"type": "mrkdwn", "text": f"*Probable cause*\n{causes}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":white_check_mark: *Recommended actions*\n{actions}",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Incident ID: `{incident.get('incident_id', '')}`",
                }
            ],
        },
    ]
    return _post_webhook(
        {
            "channel": _channel(),
            "blocks": blocks,
            "text": f"Incident alert — {severity.upper()} — {scope}",
        }
    )
