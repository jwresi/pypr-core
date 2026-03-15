# PYPR Contract Schemas

These schemas are implementation-neutral contracts for PYPR internals.

## Required contracts

- `signal_event.schema.json`
- `state_assessment.schema.json`
- `confidence_assessment.schema.json`
- `memory_record.schema.json`
- `pattern_record.schema.json`
- `decision_record.schema.json`
- `intervention_decision.schema.json`

## Validation rule

Any PYPR runtime should validate these object types at service boundaries:
- Signal ingest
- State publication
- Memory writes
- Intervention dispatch

## Design intent

The schemas protect behavior identity while allowing replaceable technology stacks.
