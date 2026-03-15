from __future__ import annotations

from packages.pypr.config import get_threshold
from packages.pypr.models import InterventionDecision, InterventionLevel, StateAssessment


def decide_intervention(assessment: StateAssessment, interruptions_last_hour: int = 0) -> InterventionDecision:
    confidence = assessment.confidence
    reasons = [f"state={assessment.state.value}", f"confidence={confidence:.2f}"]

    budget = int(get_threshold(["thresholds", "patience", "interruption_budget_per_hour"], 6))
    suppress_below = get_threshold(["thresholds", "patience", "low_relevance_suppress_below"], 0.65)

    if interruptions_last_hour >= budget and confidence < 0.9:
        return InterventionDecision(
            customer_id=assessment.customer_id,
            level=InterventionLevel.silent,
            confidence=confidence,
            message="Patience gate active: holding intervention to reduce operator load.",
            reasons=reasons + ["interruption budget exhausted"],
        )

    if "telemetry_sparse" in assessment.failure_modes and confidence < 0.9:
        return InterventionDecision(
            customer_id=assessment.customer_id,
            level=InterventionLevel.silent,
            confidence=confidence,
            message="No intervention: sparse telemetry lowers action confidence.",
            reasons=reasons + ["failure_mode=telemetry_sparse"],
        )

    if "signal_conflict" in assessment.failure_modes and confidence < 0.9:
        reasons.append("failure_mode=signal_conflict")

    if confidence < suppress_below and assessment.state.value in {"stable", "unknown"}:
        return InterventionDecision(
            customer_id=assessment.customer_id,
            level=InterventionLevel.silent,
            confidence=confidence,
            message="No intervention: low relevance and low confidence.",
            reasons=reasons + ["suppressed by patience threshold"],
        )

    nudge_min = get_threshold(["thresholds", "intervention", "nudge_min"], 0.4)
    suggestion_min = get_threshold(["thresholds", "intervention", "suggestion_min"], 0.6)
    warning_min = get_threshold(["thresholds", "intervention", "warning_min"], 0.78)
    critical_min = get_threshold(["thresholds", "intervention", "critical_min"], 0.9)

    if confidence >= critical_min:
        level = InterventionLevel.critical
        message = f"Critical: {assessment.summary}"
    elif confidence >= warning_min:
        level = InterventionLevel.warning
        message = f"Warning: {assessment.summary}"
    elif confidence >= suggestion_min:
        level = InterventionLevel.suggestion
        message = f"Suggestion: {assessment.summary}"
    elif confidence >= nudge_min:
        level = InterventionLevel.nudge
        message = f"Nudge: {assessment.summary}"
    else:
        level = InterventionLevel.silent
        message = "No intervention: insufficient confidence."

    if "signal_conflict" in assessment.failure_modes and level in {InterventionLevel.warning, InterventionLevel.critical}:
        level = InterventionLevel.suggestion
        message = "Suggestion: conflicting signals detected; gather more telemetry before escalation."

    return InterventionDecision(
        customer_id=assessment.customer_id,
        level=level,
        confidence=confidence,
        message=message,
        reasons=reasons,
    )
