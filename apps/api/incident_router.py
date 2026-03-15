from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from apps.api.jake_router import _ops
from packages.jake.incidents.engine import correlate_from_jake, create_incident

router = APIRouter(prefix="/v1/incidents", tags=["incidents"])


class IncidentRequest(BaseModel):
    scope: str
    signals: list[dict] = Field(default_factory=list)


@router.post("/correlate", summary="Auto-correlate signals for a scope into an incident")
def correlate(scope: str) -> dict:
    try:
        return correlate_from_jake(scope, _ops())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/create", summary="Create incident from provided signals")
def create(req: IncidentRequest) -> dict:
    try:
        return create_incident(req.scope, req.signals, _ops())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
