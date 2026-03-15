from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Iterator

from packages.pypr.models import MemoryItem, MemoryRecord, Signal


DB_PATH = os.getenv("PYPR_DB_PATH", "data/pypr.db")


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                status TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                key TEXT NOT NULL,
                value_json TEXT NOT NULL,
                confidence REAL NOT NULL,
                source TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_signals_customer_time
            ON signals (customer_id, observed_at DESC);

            CREATE INDEX IF NOT EXISTS idx_memory_key
            ON memory (key);

            CREATE INDEX IF NOT EXISTS idx_memory_created
            ON memory (created_at DESC);
            """
        )


@contextmanager
def _conn() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def persist_signal(signal: Signal) -> None:
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO signals (customer_id, signal_type, status, observed_at, metadata_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                signal.customer_id,
                signal.signal_type.value,
                signal.status,
                signal.observed_at.isoformat(),
                json.dumps(signal.metadata),
            ),
        )
        conn.commit()


def recent_signals(customer_id: str, limit: int = 50) -> list[Signal]:
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT customer_id, signal_type, status, observed_at, metadata_json
            FROM signals
            WHERE customer_id = ?
            ORDER BY observed_at DESC
            LIMIT ?
            """,
            (customer_id, limit),
        ).fetchall()

    return [
        Signal(
            customer_id=row[0],
            signal_type=row[1],
            status=row[2],
            observed_at=datetime.fromisoformat(row[3]),
            metadata=json.loads(row[4]),
        )
        for row in rows
    ]


def persist_memory(record: MemoryRecord) -> None:
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO memory (kind, key, value_json, confidence, source, tags_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.kind,
                record.key,
                json.dumps(record.value),
                record.confidence,
                record.source,
                json.dumps(record.tags),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()


def query_memory(
    kind: str | None = None,
    key_prefix: str | None = None,
    tag: str | None = None,
    min_confidence: float = 0.0,
    limit: int = 50,
) -> list[MemoryItem]:
    where = ["confidence >= ?"]
    params: list[object] = [min_confidence]

    if kind:
        where.append("kind = ?")
        params.append(kind)
    if key_prefix:
        where.append("key LIKE ?")
        params.append(f"{key_prefix}%")
    if tag:
        where.append("instr(tags_json, ?) > 0")
        params.append(f'"{tag}"')

    sql = f"""
        SELECT kind, key, value_json, confidence, source, tags_json, created_at
        FROM memory
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT ?
    """
    params.append(limit)

    with _conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [
        MemoryItem(
            kind=row[0],
            key=row[1],
            value=json.loads(row[2]),
            confidence=float(row[3]),
            source=row[4],
            tags=json.loads(row[5]),
            created_at=datetime.fromisoformat(row[6]),
        )
        for row in rows
    ]


def count_recent_interventions(customer_id: str, hours: int = 1) -> int:
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    key = f"customer:{customer_id}:intervention"

    with _conn() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM memory
            WHERE key = ? AND created_at >= ?
            """,
            (key, since),
        ).fetchone()

    return int(row[0]) if row else 0
