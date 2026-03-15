from __future__ import annotations

from fastapi import FastAPI

from apps.api.jake_router import router as jake_router
from packages.pypr.config import load_policy
from packages.pypr.intervention import decide_intervention
from packages.pypr.memory import (
    count_recent_interventions,
    init_db,
    persist_memory,
    persist_signal,
    query_memory,
    recent_signals,
)
from packages.pypr.models import IngestResponse, MemoryRecord, MemorySearchQuery, Signal, StateAssessment
from packages.pypr.observation import infer_state
from packages.pypr.reasoning import explain_assessment
from packages.pypr.slack_adapter import router as slack_router

app = FastAPI(
    title="pypr-core",
    version="0.1.0",
    description="Jake network operations + PYPR behavioral layer",
)

app.include_router(slack_router)
app.include_router(jake_router)


@app.on_event("startup")
def startup() -> None:
    init_db()


# ── platform health ────────────────────────────────────────────────────────────

@app.get("/health", tags=["platform"])
def health() -> dict[str, str]:
    return {"status": "ok", "identity": "pypr-core"}


@app.get("/v1/policy", tags=["platform"])
def policy() -> dict:
    return load_policy()


# ── PYPR signal ingestion ──────────────────────────────────────────────────────

@app.post("/v1/signals/ingest", response_model=IngestResponse, tags=["pypr"])
def ingest_signal(signal: Signal) -> IngestResponse:
    persist_signal(signal)

    signals = recent_signals(signal.customer_id)
    assessment = infer_state(signal.customer_id, signals)

    persist_memory(
        MemoryRecord(
            kind="event",
            key=f"customer:{signal.customer_id}:state",
            value={
                "state": assessment.state.value,
                "summary": assessment.summary,
                "evidence": assessment.evidence,
                "failure_modes": assessment.failure_modes,
                "uncertainty": assessment.uncertainty,
                "reasoning": explain_assessment(assessment),
            },
            confidence=assessment.confidence,
            source="observation+reasoning",
            tags=["customer-state", assessment.state.value],
        )
    )

    interruptions_last_hour = count_recent_interventions(signal.customer_id)
    intervention = decide_intervention(assessment, interruptions_last_hour=interruptions_last_hour)

    persist_memory(
        MemoryRecord(
            kind="event",
            key=f"customer:{signal.customer_id}:intervention",
            value={
                "level": intervention.level.value,
                "message": intervention.message,
                "reasons": intervention.reasons,
            },
            confidence=intervention.confidence,
            source="intervention-engine",
            tags=["intervention", intervention.level.value],
        )
    )

    return IngestResponse(assessment=assessment, intervention=intervention)


@app.get("/v1/customers/{customer_id}/state", response_model=StateAssessment, tags=["pypr"])
def customer_state(customer_id: str) -> StateAssessment:
    signals = recent_signals(customer_id)
    return infer_state(customer_id, signals)


@app.post("/v1/memory", tags=["pypr"])
def write_memory(record: MemoryRecord) -> dict[str, str]:
    persist_memory(record)
    return {"status": "stored"}


@app.post("/v1/memory/search", tags=["pypr"])
def search_memory(query: MemorySearchQuery) -> dict:
    items = query_memory(
        kind=query.kind,
        key_prefix=query.key_prefix,
        tag=query.tag,
        min_confidence=query.min_confidence,
        limit=query.limit,
    )
    return {"count": len(items), "items": [item.model_dump(mode="json") for item in items]}


@app.get("/v1/customers/{customer_id}/timeline", tags=["pypr"])
def customer_timeline(customer_id: str, limit: int = 50) -> dict:
    items = query_memory(key_prefix=f"customer:{customer_id}:", limit=limit)
    return {
        "customer_id": customer_id,
        "count": len(items),
        "items": [item.model_dump(mode="json") for item in items],
    }
