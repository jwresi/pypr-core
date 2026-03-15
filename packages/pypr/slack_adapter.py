from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
import urllib.request
from typing import Any

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from packages.pypr.slack_commands import SlackCommandRouter

router = APIRouter(prefix="/v1/slack", tags=["slack"])


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _router() -> SlackCommandRouter:
    base_url = os.getenv("PYPR_BASE_URL", "http://localhost:8080")
    read_only = _bool_env("PYPR_SLACK_READ_ONLY", True)
    return SlackCommandRouter(base_url=base_url, read_only=read_only)


def _verify_signature(
    body: bytes,
    slack_signature: str | None,
    slack_request_timestamp: str | None,
) -> bool:
    secret = os.getenv("PYPR_SLACK_SIGNING_SECRET", "").strip()
    if not secret:
        return True

    if not slack_signature or not slack_request_timestamp:
        return False

    try:
        ts = int(slack_request_timestamp)
    except ValueError:
        return False

    # Slack replay protection window: 5 minutes.
    if abs(time.time() - ts) > 60 * 5:
        return False

    basestring = f"v0:{slack_request_timestamp}:{body.decode('utf-8')}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), basestring, hashlib.sha256).hexdigest()
    expected = f"v0={digest}"
    return hmac.compare_digest(expected, slack_signature)


def _extract_slash_command(text: str) -> str | None:
    clean = text.strip()
    if not clean:
        return None

    # Strip leading bot mention token, if present.
    if clean.startswith("<@"):
        parts = clean.split(maxsplit=1)
        clean = parts[1].strip() if len(parts) > 1 else ""

    if not clean.startswith("/"):
        return None
    return clean


def _post_to_slack(channel: str, text: str, thread_ts: str | None = None) -> None:
    token = os.getenv("PYPR_SLACK_BOT_TOKEN", "").strip()
    if not token:
        return

    payload: dict[str, Any] = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts

    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "content-type": "application/json",
            "authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        _ = resp.read()


def _format_result(result: dict[str, Any]) -> str:
    if result.get("ok"):
        return "```" + json.dumps(result["data"], indent=2, sort_keys=True) + "```"
    return f"Command error: {result.get('error', 'unknown error')}"


@router.get("/health")
def slack_health() -> dict[str, Any]:
    return {
        "status": "ok",
        "adapter": "slack-events",
        "read_only": _bool_env("PYPR_SLACK_READ_ONLY", True),
        "signature_verification": bool(os.getenv("PYPR_SLACK_SIGNING_SECRET", "").strip()),
        "bot_token_configured": bool(os.getenv("PYPR_SLACK_BOT_TOKEN", "").strip()),
    }


@router.post("/events")
async def slack_events(
    request: Request,
    x_slack_signature: str | None = Header(default=None),
    x_slack_request_timestamp: str | None = Header(default=None),
) -> JSONResponse:
    body = await request.body()
    if not _verify_signature(body, x_slack_signature, x_slack_request_timestamp):
        return JSONResponse(status_code=401, content={"ok": False, "error": "invalid slack signature"})

    payload = json.loads(body.decode("utf-8"))

    if payload.get("type") == "url_verification":
        return JSONResponse(content={"challenge": payload.get("challenge", "")})

    if payload.get("type") != "event_callback":
        return JSONResponse(content={"ok": True, "ignored": True, "reason": "unsupported payload type"})

    event = payload.get("event", {})
    event_type = event.get("type", "")
    subtype = event.get("subtype")

    if subtype == "bot_message":
        return JSONResponse(content={"ok": True, "ignored": True, "reason": "bot message"})

    if event_type not in {"app_mention", "message"}:
        return JSONResponse(content={"ok": True, "ignored": True, "reason": "unsupported event"})

    text = event.get("text", "")
    command = _extract_slash_command(text)
    if not command:
        return JSONResponse(content={"ok": True, "ignored": True, "reason": "no slash command"})

    try:
        result = await asyncio.to_thread(_router().run_command, command)
    except RuntimeError as exc:
        return JSONResponse(content={"ok": False, "error": str(exc)})
    rendered = _format_result(result)

    channel = event.get("channel")
    thread_ts = event.get("thread_ts") or event.get("ts")
    if channel:
        try:
            await asyncio.to_thread(_post_to_slack, channel=channel, text=rendered, thread_ts=thread_ts)
        except Exception as exc:
            # Keep Slack event ack success even if post-back fails.
            return JSONResponse(content={"ok": False, "error": f"post_message_failed: {exc}", "result": result})

    return JSONResponse(content={"ok": True, "result": result})


@router.post("/simulate")
def simulate_slack_command(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text", ""))
    command = _extract_slash_command(text)
    if not command:
        return {"ok": False, "error": "text must contain slash command, e.g. '/health'"}
    return _router().run_command(command)
