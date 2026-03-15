from __future__ import annotations


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def score_confidence(
    base: float,
    signal_count: int,
    source_diversity: int,
    has_conflict: bool,
    has_sparse_telemetry: bool,
) -> float:
    score = base
    score += min(0.2, 0.03 * signal_count)
    score += min(0.15, 0.05 * max(0, source_diversity - 1))

    if has_conflict:
        score -= 0.25
    if has_sparse_telemetry:
        score -= 0.15

    return clamp(score, 0.05, 0.99)


def describe_uncertainty(confidence: float, failure_modes: list[str]) -> str:
    if confidence >= 0.85 and not failure_modes:
        return "Low uncertainty"
    if confidence >= 0.65:
        return "Moderate uncertainty"
    return "High uncertainty"
