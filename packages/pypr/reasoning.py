from __future__ import annotations

from packages.pypr.models import StateAssessment


def explain_assessment(assessment: StateAssessment) -> str:
    confidence_band = (
        "high" if assessment.confidence >= 0.85 else "moderate" if assessment.confidence >= 0.65 else "low"
    )
    failure_clause = (
        f" Failure modes: {', '.join(assessment.failure_modes)}."
        if assessment.failure_modes
        else ""
    )
    return (
        f"State={assessment.state.value}; confidence={assessment.confidence:.2f} ({confidence_band}); "
        f"uncertainty={assessment.uncertainty}. Evidence: {', '.join(assessment.evidence)}."
        f"{failure_clause}"
    )
