from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

_DB_PATH = Path(os.environ.get("JAKE_OPS_DB", "network_map.db"))


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(_DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def init_incidents_table() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                id          TEXT PRIMARY KEY,
                scope       TEXT NOT NULL,
                severity    TEXT NOT NULL,
                status      TEXT NOT NULL,
                started_at  TEXT NOT NULL,
                resolved_at TEXT,
                signal_types TEXT,
                data_json   TEXT NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_incidents_scope ON incidents(scope)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_incidents_started ON incidents(started_at)")


def save_incident(incident: dict[str, Any]) -> dict[str, Any]:
    with _conn() as con:
        con.execute("""
            INSERT INTO incidents (id, scope, severity, status, started_at, resolved_at, signal_types, data_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                severity    = excluded.severity,
                status      = excluded.status,
                resolved_at = excluded.resolved_at,
                signal_types = excluded.signal_types,
                data_json   = excluded.data_json
        """, (
            incident["incident_id"],
            incident["scope"],
            incident["severity"],
            incident["status"],
            incident["started_at"],
            incident.get("resolved_at"),
            json.dumps(incident.get("signal_types", [])),
            json.dumps(incident),
        ))
    return incident


def get_incident(incident_id: str) -> dict[str, Any] | None:
    with _conn() as con:
        row = con.execute(
            "SELECT data_json FROM incidents WHERE id = ?", (incident_id,)
        ).fetchone()
    return json.loads(row["data_json"]) if row else None


def list_incidents(
    scope: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if scope:
        clauses.append("scope = ?")
        params.append(scope)
    if status:
        clauses.append("status = ?")
        params.append(status)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)
    with _conn() as con:
        rows = con.execute(
            f"SELECT data_json FROM incidents {where} ORDER BY started_at DESC LIMIT ?",
            params,
        ).fetchall()
    return [json.loads(r["data_json"]) for r in rows]


def update_incident_status(incident_id: str, status: str, resolved_at: str | None = None) -> dict[str, Any] | None:
    incident = get_incident(incident_id)
    if not incident:
        return None
    incident["status"] = status
    if resolved_at:
        incident["resolved_at"] = resolved_at
    save_incident(incident)
    return incident


def add_note(incident_id: str, note: str) -> dict[str, Any] | None:
    incident = get_incident(incident_id)
    if not incident:
        return None
    incident.setdefault("notes", []).append({"text": note, "at": __import__("datetime").datetime.utcnow().isoformat()})
    save_incident(incident)
    return incident


def incident_timeline(scope: str, limit: int = 100) -> list[dict[str, Any]]:
    """All incidents for a scope, ordered oldest first — for outage reconstruction."""
    with _conn() as con:
        rows = con.execute(
            "SELECT data_json FROM incidents WHERE scope = ? ORDER BY started_at ASC LIMIT ?",
            (scope, limit),
        ).fetchall()
    return [json.loads(r["data_json"]) for r in rows]
