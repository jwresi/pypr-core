# PYPR Architecture (Stack-Agnostic)

PYPR is defined by behavior and contracts, not vendor choices.

## Core Systems

1. Observation Engine
- Ingests heterogeneous signals.
- Normalizes into canonical `SignalEvent`.
- Produces abstracted state candidates instead of raw telemetry dumps.

2. Memory System
- Stores selective high-signal cognition artifacts.
- Supports persistence, retrieval, and relevance filtering.
- First-class objects: facts, events, patterns, decisions, commitments.

3. Reasoning Layer
- Builds context from memory + current observations.
- Generates hypothesis and confidence.
- Must expose uncertainty and evidence.

4. Proactivity + Patience Engine
- Chooses if/when/how to interrupt.
- Uses confidence, relevance, workload, and historical noise.
- Escalation ladder: silent -> nudge -> suggestion -> warning -> critical.

## Behavioral Laws

- Correlation over speculation.
- Confidence and evidence for every material claim.
- Silence by default under low confidence or high interruption cost.
- Memory hygiene over hoarding.
- Explainability over verbosity.

## Contract-First Build Rule

Any runtime (Python, Node, Go) must implement the schema contracts in `pypr/schemas/contracts/`:
- `signal_event.schema.json`
- `state_assessment.schema.json`
- `memory_record.schema.json`
- `pattern_record.schema.json`
- `decision_record.schema.json`
- `confidence_assessment.schema.json`
- `intervention_decision.schema.json`

These contracts define PYPR identity. Implementations are replaceable.
