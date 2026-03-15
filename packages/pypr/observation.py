from __future__ import annotations

from collections import Counter

from packages.pypr.confidence import describe_uncertainty, score_confidence
from packages.pypr.models import CustomerState, Signal, StateAssessment


OFFLINE_HINTS = {"down", "missing", "expired", "stale", "offline", "failed"}
DEGRADED_HINTS = {"high-latency", "loss", "weak", "degraded", "congested"}
INTERMITTENT_HINTS = {"flap", "unstable", "intermittent"}
ONLINE_HINTS = {"up", "online", "ok", "active", "bound"}


def detect_failure_modes(signals: list[Signal], offline_score: int) -> list[str]:
    failure_modes: list[str] = []
    statuses = {s.status.strip().lower() for s in signals}
    source_diversity = len({s.signal_type.value for s in signals})

    has_online = any(st in ONLINE_HINTS for st in statuses)
    has_offline = offline_score > 0

    if len(signals) < 3:
        failure_modes.append("telemetry_sparse")
    if has_online and has_offline:
        failure_modes.append("signal_conflict")
    if len(signals) >= 4 and source_diversity == 1:
        failure_modes.append("single_source_dominance")

    return failure_modes


def infer_state(customer_id: str, signals: list[Signal]) -> StateAssessment:
    if not signals:
        return StateAssessment(
            customer_id=customer_id,
            state=CustomerState.unknown,
            confidence=0.2,
            evidence=["No signals available"],
            failure_modes=["telemetry_absent"],
            uncertainty="High uncertainty",
            summary="Customer state unknown due to missing telemetry.",
        )

    statuses = [s.status.strip().lower() for s in signals]
    counts = Counter(statuses)

    offline_score = sum(count for status, count in counts.items() if status in OFFLINE_HINTS)
    degraded_score = sum(count for status, count in counts.items() if status in DEGRADED_HINTS)
    intermittent_score = sum(count for status, count in counts.items() if status in INTERMITTENT_HINTS)

    total = max(len(statuses), 1)
    source_diversity = len({s.signal_type.value for s in signals})

    failure_modes = detect_failure_modes(signals, offline_score)
    has_conflict = "signal_conflict" in failure_modes
    has_sparse = "telemetry_sparse" in failure_modes

    state = CustomerState.stable
    base = 0.55
    summary = "Customer appears stable based on recent telemetry."

    if offline_score >= 2:
        state = CustomerState.offline
        base = 0.62
        summary = "Customer appears offline with correlated link/session failures."
    elif intermittent_score >= 2:
        state = CustomerState.intermittent
        base = 0.58
        summary = "Customer shows flapping or unstable behavior across recent telemetry."
    elif degraded_score >= 1 or offline_score == 1:
        state = CustomerState.degraded
        base = 0.52
        summary = "Customer is online but performance appears degraded or unstable."

    confidence = score_confidence(
        base=base,
        signal_count=total,
        source_diversity=source_diversity,
        has_conflict=has_conflict,
        has_sparse_telemetry=has_sparse,
    )

    evidence = [
        f"offline indicators={offline_score}",
        f"intermittent indicators={intermittent_score}",
        f"degraded indicators={degraded_score}",
        f"signals analyzed={total}",
        f"source diversity={source_diversity}",
    ]

    return StateAssessment(
        customer_id=customer_id,
        state=state,
        confidence=confidence,
        evidence=evidence,
        failure_modes=failure_modes,
        uncertainty=describe_uncertainty(confidence, failure_modes),
        summary=summary,
    )
