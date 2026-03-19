from __future__ import annotations

import json
import math
import os
import shlex
import subprocess
import threading
import time
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any

from packages.jake.connectors.mcp.jake_ops_mcp import load_local_env_file, load_transport_radio_scan


def _safe_ip(ip: str) -> str:
    text = str(ip or "").strip()
    if not text or any(ch not in "0123456789." for ch in text):
        raise ValueError("Invalid Siklu IP")
    return text


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _command_template() -> str:
    load_local_env_file()
    return os.environ.get("SIKLU_ALIGN_COMMAND", "").strip()


def _run_command_collector(ip: str) -> dict[str, Any] | None:
    template = _command_template()
    if not template:
        return None
    command = template.format(ip=_safe_ip(ip))
    completed = subprocess.run(
        shlex.split(command),
        capture_output=True,
        text=True,
        timeout=2.5,
        check=False,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return {
            "source": "command",
            "collector_ok": False,
            "collector_error": completed.stderr.strip() or f"command exited {completed.returncode}",
        }
    payload = json.loads(completed.stdout)
    payload["source"] = payload.get("source") or "command"
    payload["collector_ok"] = True
    return payload


def _artifact_row(ip: str) -> dict[str, Any] | None:
    for row in load_transport_radio_scan().get("results") or []:
        if str(row.get("type") or "").lower() == "siklu" and str(row.get("ip") or "").strip() == ip:
            return row
    return None


def _artifact_snapshot(ip: str) -> dict[str, Any]:
    row = _artifact_row(ip)
    if not row:
        return {
            "ip": ip,
            "source": "artifact",
            "collector_ok": False,
            "collector_error": "Siklu radio not found in transport scan artifact",
        }
    alignment_text = " ".join(
        [
            str(row.get("process-status") or ""),
            str(row.get("linux-process-status") or ""),
            str(row.get("show-log") or ""),
            json.dumps(row.get("log_analysis") or {}),
        ]
    ).lower()
    return {
        "ip": ip,
        "name": row.get("name"),
        "model": row.get("model"),
        "location": row.get("location"),
        "status": row.get("status"),
        "source": "artifact",
        "collector_ok": True,
        "alignment_mode": "alignment" in alignment_text,
        "current_rssi": _coerce_float(row.get("current_rssi") or row.get("rssi")),
        "current_cinr": _coerce_float(row.get("current_cinr") or row.get("cinr")),
        "max_seen_rssi": _coerce_float(row.get("max_seen_rssi")),
        "expected_rssi": _coerce_float(row.get("expected_rssi")),
        "raw": {
            "log_analysis": row.get("log_analysis"),
            "process_status": row.get("process-status"),
            "linux_process_status": row.get("linux-process-status"),
        },
    }


def _numeric_text(value: Any) -> float | None:
    text = str(value or "").strip().lower()
    if not text or text in {"n/a", "na", "unknown"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _html_login_error(payload: str) -> str | None:
    text = str(payload or "")
    lowered = text.lower()
    if "<!doctype html" not in lowered and "<html" not in lowered:
        return None
    if "too many sessions" in lowered:
        return "Too many sessions"
    if "username" in lowered and "password" in lowered and "login" in lowered:
        return "Login page returned instead of telemetry"
    return "HTML login page returned instead of telemetry"


def _webui_login(ip: str, username: str, password: str, cookie_path: str, timeout: float) -> dict[str, Any] | None:
    login = subprocess.run(
        [
            "curl",
            "-k",
            "-s",
            "-c",
            cookie_path,
            "-d",
            f"user={username}&password={password}&caller_url=/",
            f"https://{ip}/handleform",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if login.returncode != 0:
        return {
            "ip": ip,
            "source": "webui",
            "collector_ok": False,
            "collector_error": login.stderr.strip() or f"curl login exited {login.returncode}",
        }
    login_error = _html_login_error(login.stdout)
    if login_error:
        return {
            "ip": ip,
            "source": "webui",
            "collector_ok": False,
            "collector_error": login_error,
            "raw_excerpt": login.stdout[-1000:],
        }
    return None


def _webui_query(ip: str, cookie_path: str, timeout: float) -> dict[str, Any]:
    query = "https://{}/main/web.cgi?mo-info%20rf%20;%20sw%20;%20extend-mm".format(ip)
    resp = subprocess.run(
        [
            "curl",
            "-k",
            "-s",
            "-b",
            cookie_path,
            query,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if resp.returncode != 0:
        return {
            "ip": ip,
            "source": "webui",
            "collector_ok": False,
            "collector_error": resp.stderr.strip() or f"curl query exited {resp.returncode}",
        }
    payload = resp.stdout
    login_error = _html_login_error(payload)
    if login_error:
        return {
            "ip": ip,
            "source": "webui",
            "collector_ok": False,
            "collector_error": login_error,
            "raw_excerpt": payload[-1000:],
        }

    xml = ET.fromstring(payload)
    rf_mo = next((node for node in xml.findall("mo") if node.attrib.get("type") == "rf"), None)
    if rf_mo is None:
        return {
            "ip": ip,
            "source": "webui",
            "collector_ok": False,
            "collector_error": "rf mo missing from web.cgi reply",
            "raw_excerpt": payload[-2000:],
        }

    attrs = {attr.attrib.get("name"): attr.attrib.get("value") for attr in rf_mo.findall("attr")}
    stats_header = rf_mo.findtext("stats-header") or ""
    stats_current = rf_mo.findtext("stats-current") or ""
    stats_map: dict[str, str] = {}
    if stats_header and stats_current:
        keys = [item.strip() for item in stats_header.split(",")]
        values = [item.strip() for item in stats_current.split(",")]
        stats_map = {keys[i]: values[i] if i < len(values) else "" for i in range(len(keys))}

    alignment_max = _numeric_text(attrs.get("alignment-max-rssi"))
    stats_max = _numeric_text(stats_map.get("max-rssi"))
    if stats_max is not None and stats_max >= 0:
        stats_max = None
    return {
        "ip": ip,
        "source": "webui",
        "collector_ok": True,
        "alignment_mode": str(attrs.get("alignment-status") or "").strip().lower() not in {"", "inactive", "disable", "disabled"},
        "alignment_status": attrs.get("alignment-status"),
        "current_rssi": _numeric_text(attrs.get("rssi")),
        "current_cinr": _numeric_text(attrs.get("cinr")),
        "max_seen_rssi": alignment_max if alignment_max is not None else stats_max,
        "expected_rssi": _numeric_text(attrs.get("expected-rssi")),
        "rf_mode": attrs.get("mode"),
        "operational": attrs.get("operational"),
        "tx_state": attrs.get("tx-state"),
        "rx_state": attrs.get("rx-state"),
        "tx_power": _numeric_text(attrs.get("tx-power")),
        "air_capacity": _numeric_text(attrs.get("air-capacity")),
        "stats": stats_map,
        "raw_excerpt": payload[-2000:],
    }


def _webui_snapshot(ip: str, cookie_path: str | None = None) -> dict[str, Any] | None:
    load_local_env_file()
    username = os.environ.get("SIKLU_USERNAME", "").strip()
    password = os.environ.get("SIKLU_PASSWORD", "").strip()
    if not username or not password:
        return None

    timeout = float(os.environ.get("SIKLU_WEBUI_TIMEOUT", "10.0"))
    owns_cookie = False
    if not cookie_path:
        fd, cookie_path = tempfile.mkstemp(prefix="siklu_cookie_")
        os.close(fd)
        owns_cookie = True
    try:
        snapshot = _webui_query(ip, cookie_path, timeout)
        if snapshot.get("collector_ok"):
            return snapshot
        if snapshot.get("collector_error") not in {
            "Too many sessions",
            "Login page returned instead of telemetry",
            "HTML login page returned instead of telemetry",
        }:
            return snapshot
        login_error = _webui_login(ip, username, password, cookie_path, timeout)
        if login_error:
            return login_error
        return _webui_query(ip, cookie_path, timeout)
    finally:
        if owns_cookie:
            try:
                os.remove(cookie_path)
            except OSError:
                pass


def poll_siklu_alignment(ip: str, cookie_path: str | None = None) -> dict[str, Any]:
    ip = _safe_ip(ip)
    try:
        webui_data = _webui_snapshot(ip, cookie_path=cookie_path)
    except Exception as exc:
        webui_data = {
            "ip": ip,
            "source": "webui",
            "collector_ok": False,
            "collector_error": str(exc),
        }
    if webui_data and webui_data.get("collector_ok"):
        return {"ip": ip, **webui_data}
    command_data = _run_command_collector(ip)
    if command_data is not None:
        return {"ip": ip, **command_data}
    artifact = _artifact_snapshot(ip)
    if webui_data and not webui_data.get("collector_ok"):
        artifact["webui_error"] = webui_data.get("collector_error")
    return artifact


@dataclass
class AlignmentSession:
    ip: str
    poll_ms: int = 750
    current: dict[str, Any] = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)
    updated_at: float = 0.0
    max_rssi_session: float | None = None
    max_cinr_session: float | None = None
    peak_hold_at: float | None = None
    recent_samples: list[dict[str, float]] = field(default_factory=list)
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    poll_lock: threading.Lock = field(default_factory=threading.Lock)
    webui_cookie_path: str | None = None


class SikluAlignmentService:
    def __init__(self) -> None:
        self._sessions: dict[str, AlignmentSession] = {}
        self._lock = threading.Lock()
        self._link_notes: dict[str, dict[str, Any]] = {}

    def ensure_session(self, ip: str, poll_ms: int = 750) -> AlignmentSession:
        safe_ip = _safe_ip(ip)
        bounded_ms = min(1000, max(250, int(poll_ms)))
        with self._lock:
            session = self._sessions.get(safe_ip)
            if session is None:
                session = AlignmentSession(ip=safe_ip, poll_ms=bounded_ms)
                self._sessions[safe_ip] = session
            else:
                session.poll_ms = bounded_ms
            return session

    def snapshot(self, ip: str, poll_ms: int = 750) -> dict[str, Any]:
        session = self.ensure_session(ip, poll_ms=poll_ms)
        if session.updated_at == 0.0:
            self._poll_once(session)
            if session.thread is None:
                session.thread = threading.Thread(target=self._run_session, args=(session,), daemon=True)
                session.thread.start()
        current = dict(session.current)
        current_rssi = _coerce_float(current.get("current_rssi"))
        delta_from_max = None
        if current_rssi is not None and session.max_rssi_session is not None:
            delta_from_max = current_rssi - session.max_rssi_session
        return {
            "ip": session.ip,
            "poll_ms": session.poll_ms,
            "started_at": session.started_at,
            "updated_at": session.updated_at,
            "current": current,
            "max_rssi_session": session.max_rssi_session,
            "max_cinr_session": session.max_cinr_session,
            "delta_from_max_rssi": delta_from_max,
            "peak_hint": self._peak_hint(session),
            "peak_hold_at": session.peak_hold_at,
        }

    def reset_peak(self, ip: str, poll_ms: int = 750) -> dict[str, Any]:
        session = self.ensure_session(ip, poll_ms=poll_ms)
        with session.poll_lock:
            current_rssi = _coerce_float(session.current.get("current_rssi"))
            current_cinr = _coerce_float(session.current.get("current_cinr"))
            session.max_rssi_session = current_rssi
            session.max_cinr_session = current_cinr
            session.peak_hold_at = session.updated_at or time.time()
            session.recent_samples = []
            if current_rssi is not None:
                session.recent_samples.append({"at": session.peak_hold_at, "rssi": current_rssi})
        return self.snapshot(ip, poll_ms=poll_ms)

    def reset_web_session(self, ip: str, poll_ms: int = 750) -> dict[str, Any]:
        session = self.ensure_session(ip, poll_ms=poll_ms)
        with session.poll_lock:
            if session.webui_cookie_path:
                try:
                    os.remove(session.webui_cookie_path)
                except OSError:
                    pass
                session.webui_cookie_path = None
            session.current = {
                "ip": session.ip,
                "source": "session",
                "collector_ok": False,
                "collector_error": "Backend web session reset; awaiting fresh login",
            }
            session.updated_at = time.time()
        return self.snapshot(ip, poll_ms=poll_ms)

    def _link_key(self, ip_a: str, ip_b: str) -> str:
        a = _safe_ip(ip_a)
        b = _safe_ip(ip_b)
        return "|".join(sorted((a, b)))

    def get_link_notes(self, ip_a: str, ip_b: str) -> dict[str, Any]:
        key = self._link_key(ip_a, ip_b)
        return dict(self._link_notes.get(key) or {"text": "", "updated_at": None})

    def save_link_notes(self, ip_a: str, ip_b: str, text: str) -> dict[str, Any]:
        key = self._link_key(ip_a, ip_b)
        payload = {"text": str(text or ""), "updated_at": time.time()}
        self._link_notes[key] = payload
        return dict(payload)

    def _poll_once(self, session: AlignmentSession) -> None:
        with session.poll_lock:
            if session.webui_cookie_path is None:
                fd, session.webui_cookie_path = tempfile.mkstemp(prefix="siklu_cookie_")
                os.close(fd)
            payload = poll_siklu_alignment(session.ip, cookie_path=session.webui_cookie_path)
            now = time.time()
            session.current = payload
            session.updated_at = now
            current_rssi = _coerce_float(payload.get("current_rssi"))
            current_cinr = _coerce_float(payload.get("current_cinr"))
            if current_rssi is not None:
                session.recent_samples.append({"at": now, "rssi": current_rssi})
                session.recent_samples = session.recent_samples[-40:]
                if session.max_rssi_session is None or current_rssi > session.max_rssi_session:
                    session.max_rssi_session = current_rssi
                    session.peak_hold_at = now
            if current_cinr is not None and (session.max_cinr_session is None or current_cinr > session.max_cinr_session):
                session.max_cinr_session = current_cinr

    def _run_session(self, session: AlignmentSession) -> None:
        while not session.stop_event.is_set():
            try:
                self._poll_once(session)
            except Exception as exc:
                session.current = {
                    "ip": session.ip,
                    "source": "session",
                    "collector_ok": False,
                    "collector_error": str(exc),
                }
                session.updated_at = time.time()
            time.sleep(session.poll_ms / 1000.0)

    def _peak_hint(self, session: AlignmentSession) -> str:
        if len(session.recent_samples) < 4:
            return "insufficient_history"
        values = [sample["rssi"] for sample in session.recent_samples[-6:]]
        slope = values[-1] - values[0]
        if math.isclose(slope, 0.0, abs_tol=0.25):
            return "flat"
        return "improving" if slope > 0 else "cooling"


_service: SikluAlignmentService | None = None
_service_lock = threading.Lock()


def get_siklu_alignment_service() -> SikluAlignmentService:
    global _service
    if _service is None:
        with _service_lock:
            if _service is None:
                _service = SikluAlignmentService()
    return _service
