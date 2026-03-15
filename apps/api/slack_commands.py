from __future__ import annotations

import os

from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse

from apps.api.jake_router import _ops
from apps.api.noc_router import briefing as current_briefing
from packages.jake.connectors import slack as slack_connector
from packages.jake.graph.health import score_building
from packages.jake.graph.topology import get_graph
from packages.jake.incidents.engine import correlate_from_jake
from packages.jake.incidents.store import incident_timeline

router = APIRouter(prefix="/v1/slack", tags=["slack-commands"])

SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET", "")


def _ephemeral(text: str) -> dict:
    return {"response_type": "ephemeral", "text": text}


def _in_channel(text: str, blocks=None) -> dict:
    response = {"response_type": "in_channel", "text": text}
    if blocks:
        response["blocks"] = blocks
    return response


@router.post("/command", summary="Slack slash command handler for /jake")
async def slack_command(
    command: str = Form(default=""),
    text: str = Form(default=""),
    user_name: str = Form(default=""),
    channel_name: str = Form(default=""),
) -> JSONResponse:
    parts = text.strip().lower().split()
    cmd = parts[0] if parts else "help"
    arg = parts[1] if len(parts) > 1 else ""

    ops = _ops()
    graph = get_graph()

    if cmd == "briefing":
        briefing = current_briefing()
        slack_connector.post_briefing(briefing)
        status = briefing.get("network_status", "unknown").upper()
        alerts = briefing.get("alerts", {}).get("count", 0)
        customers = briefing.get("customers", {}).get("online", 0)
        return JSONResponse(
            _in_channel(
                f"NOC Briefing posted — {status} — {alerts} alerts — {customers} customers online"
            )
        )

    if cmd == "status":
        briefing = current_briefing()
        status = briefing.get("network_status", "unknown")
        alerts = briefing.get("alerts", {}).get("count", 0)
        names = ", ".join(briefing.get("alerts", {}).get("names", []))
        customers = briefing.get("customers", {}).get("online", 0)
        emoji = {
            "nominal": ":large_green_circle:",
            "warning": ":large_yellow_circle:",
            "degraded": ":red_circle:",
        }.get(status, ":white_circle:")
        return JSONResponse(
            _in_channel(
                f"{emoji} *{status.upper()}* — {alerts} alerts ({names}) — {customers} customers online"
            )
        )

    if cmd == "alerts":
        try:
            result = ops.get_site_alerts("000007")
            active = result.get("alerts") or result.get("active_alerts") or []
            if not active:
                return JSONResponse(_in_channel(":large_green_circle: No active alerts."))
            lines = [f":bell: *{len(active)} active alerts*"]
            for alert in active[:5]:
                name = (alert.get("labels") or {}).get("alertname", "unknown")
                summary = (alert.get("annotations") or {}).get("summary", "")
                lines.append(f"• {name}" + (f" — {summary}" if summary else ""))
            return JSONResponse(_in_channel("\n".join(lines)))
        except Exception as exc:
            return JSONResponse(_ephemeral(f"Error: {exc}"))

    if cmd == "site":
        scope = arg or "000007"
        try:
            summary = ops.get_site_summary(scope, include_alerts=False)
            devices = summary.get("devices_total", 0)
            switches = summary.get("switches_count", 0)
            return JSONResponse(
                _in_channel(f":building_construction: *Site {scope}* — {devices} devices, {switches} switches")
            )
        except Exception as exc:
            return JSONResponse(_ephemeral(f"Error: {exc}"))

    if cmd == "building":
        building_id = arg or "000007.055"
        try:
            ops.get_building_health(building_id, include_alerts=True)
            score_data = score_building(building_id, ops)
            score = score_data.get("score", "?")
            risk = score_data.get("risk", "?")
            factors = ", ".join(factor.get("factor", "") for factor in score_data.get("factors", []))
            emoji = {
                "low": ":large_green_circle:",
                "medium": ":large_yellow_circle:",
                "high": ":red_circle:",
                "critical": ":rotating_light:",
            }.get(risk, ":white_circle:")
            message = f"{emoji} *Building {building_id}* — Score: {score}/100 — Risk: {risk.upper()}"
            if factors:
                message += f"\nFactors: {factors}"
            return JSONResponse(_in_channel(message))
        except Exception as exc:
            return JSONResponse(_ephemeral(f"Error: {exc}"))

    if cmd == "incident":
        scope = arg or "000007.055"
        try:
            incident = correlate_from_jake(scope, ops)
            slack_connector.post_incident_alert(incident)
            severity = incident.get("severity", "?")
            signals = ", ".join(incident.get("signal_types", []))
            return JSONResponse(
                _in_channel(
                    f":rotating_light: Incident correlated for *{scope}* — "
                    f"{severity.upper()} — {signals or 'no signals'} — alert posted to channel"
                )
            )
        except Exception as exc:
            return JSONResponse(_ephemeral(f"Error: {exc}"))

    if cmd == "spofs":
        if graph._built:
            spofs = [
                node
                for node in graph.g.nodes
                if graph.g.nodes[node].get("type") in ("switch", "router")
                and len(graph.uplinks_of(node)) <= 1
            ]
            top = spofs[:10]
            lines = [f":warning: *{len(spofs)} single points of failure*"]
            lines += [f"• `{spof}`" for spof in top]
            if len(spofs) > 10:
                lines.append(f"_...and {len(spofs) - 10} more_")
            return JSONResponse(_in_channel("\n".join(lines)))
        return JSONResponse(_ephemeral("Graph not built — POST /v1/graph/sync first"))

    if cmd == "reconstruct":
        scope = arg or "000007.055"
        items = incident_timeline(scope)
        if not items:
            return JSONResponse(_in_channel(f"No incident history for `{scope}`."))
        lines = [f":scroll: *Incident history for {scope}* — {len(items)} event(s)"]
        for incident in items[:5]:
            ts = incident.get("started_at", "")[:16]
            sev = incident.get("severity", "?")
            signals = ", ".join(incident.get("signal_types", []))
            status = incident.get("status", "?")
            lines.append(f"• `{ts}Z` — {sev.upper()} — {signals or 'unknown'} — {status}")
        return JSONResponse(_in_channel("\n".join(lines)))

    return JSONResponse(
        _ephemeral(
            "*Jake commands:*\n"
            "• `/jake status` — one-line network status\n"
            "• `/jake briefing` — post full briefing\n"
            "• `/jake alerts` — active alerts\n"
            "• `/jake site 000007` — site summary\n"
            "• `/jake building 000007.055` — building health + score\n"
            "• `/jake incident 000007.055` — correlate incident\n"
            "• `/jake spofs` — single points of failure\n"
            "• `/jake reconstruct 000007.055` — incident history"
        )
    )
