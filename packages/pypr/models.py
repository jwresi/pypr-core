from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SignalType(str, Enum):
    dhcp = "dhcp"
    pppoe = "pppoe"
    arp = "arp"
    snmp = "snmp"
    syslog = "syslog"
    api = "api"
    slack = "slack"
    config = "config"


class Signal(BaseModel):
    customer_id: str = Field(min_length=1)
    signal_type: SignalType
    status: str = Field(min_length=1)
    observed_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CustomerState(str, Enum):
    stable = "stable"
    offline = "offline"
    intermittent = "intermittent"
    degraded = "degraded"
    unknown = "unknown"


class StateAssessment(BaseModel):
    customer_id: str
    state: CustomerState
    confidence: float
    evidence: list[str]
    failure_modes: list[str] = Field(default_factory=list)
    uncertainty: str = "Moderate uncertainty"
    summary: str


class MemoryRecord(BaseModel):
    kind: str
    key: str
    value: Any
    confidence: float = Field(ge=0, le=1)
    source: str
    tags: list[str] = Field(default_factory=list)


class MemoryItem(BaseModel):
    kind: str
    key: str
    value: Any
    confidence: float
    source: str
    tags: list[str]
    created_at: datetime


class MemorySearchQuery(BaseModel):
    kind: str | None = None
    key_prefix: str | None = None
    tag: str | None = None
    min_confidence: float = Field(default=0.0, ge=0, le=1)
    limit: int = Field(default=50, ge=1, le=500)


class InterventionLevel(str, Enum):
    silent = "silent"
    nudge = "nudge"
    suggestion = "suggestion"
    warning = "warning"
    critical = "critical"


class InterventionDecision(BaseModel):
    customer_id: str
    level: InterventionLevel
    confidence: float
    message: str
    reasons: list[str]


class IngestResponse(BaseModel):
    assessment: StateAssessment
    intervention: InterventionDecision
