from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from apps.api.jake_router import _ops
from packages.jake.connectors import slack as slack_connector
from packages.jake.incidents.engine import correlate_from_jake, create_incident
from packages.jake.incidents import store as incident_store

router = APIRouter(prefix="/v1/incidents", tags=["incidents"])


class IncidentRequest(BaseModel):
    scope: str
    signals: list[dict] = Field(default_factory=list)


class StatusUpdate(BaseModel):
    status: str
    resolved_at: str | None = None


class NoteRequest(BaseModel):
    note: str


@router.post("/correlate", summary="Auto-correlate signals for a scope into an incident")
def correlate(scope: str) -> dict:
    try:
        incident = correlate_from_jake(scope, _ops())
        slack_connector.post_incident_alert(incident)
        return incident
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/create", summary="Create incident from provided signals")
def create(req: IncidentRequest) -> dict:
    try:
        return create_incident(req.scope, req.signals, _ops())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", summary="List incidents")
def list_incidents(scope: str | None = None, status: str | None = None, limit: int = 50) -> dict:
    items = incident_store.list_incidents(scope=scope, status=status, limit=limit)
    return {"count": len(items), "incidents": items}


@router.get("/timeline/{scope}", summary="Full incident history for a scope")
def timeline(scope: str, limit: int = 100) -> dict:
    items = incident_store.incident_timeline(scope, limit=limit)
    return {"scope": scope, "count": len(items), "timeline": items}


@router.get("/{incident_id}", summary="Get a single incident")
def get_incident(incident_id: str) -> dict:
    incident = incident_store.get_incident(incident_id)
    if not incident:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id!r} not found")
    return incident


@router.patch("/{incident_id}/status", summary="Update incident status")
def update_status(incident_id: str, req: StatusUpdate) -> dict:
    incident = incident_store.update_incident_status(incident_id, req.status, req.resolved_at)
    if not incident:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id!r} not found")
    return incident


@router.post("/{incident_id}/notes", summary="Add a note to an incident")
def add_note(incident_id: str, req: NoteRequest) -> dict:
    incident = incident_store.add_note(incident_id, req.note)
    if not incident:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id!r} not found")
    return incident
