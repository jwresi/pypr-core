# pypr-core

Monorepo skeleton for the merged Jake and PYPR codebases.

## Layout

- `apps/api`: FastAPI entrypoint for the imported PYPR service
- `apps/ui`: imported Jake UI (Vite + React)
- `packages/jake`: deterministic Jake query and MCP connector code
- `packages/pypr`: PYPR reasoning, memory, schemas, and Slack adapter code
- `docs`: imported operational and architecture docs
- `tests/jake`: imported Jake regression scripts

## Python Setup

Use Python 3.12.

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Run the API:

```bash
uvicorn apps.api.main:app --reload
```

## Jake Prerequisites

The imported Jake regression scripts and MCP logic expect local operational data and, for some workflows, external service credentials.

- `network_map.db` at the repo root, or `JAKE_OPS_DB` pointing to it
- optional `.env` at the repo root, or `JAKE_ENV_FILE` pointing to it
- NetBox and other service credentials if you want the full Jake workflows instead of offline-only behavior

Run the imported Jake scripts from the repo root:

```bash
.venv/bin/python tests/jake/run_jake_regression_suite.py
.venv/bin/python tests/jake/run_rename_sheet_regression.py
```

## UI Setup

```bash
cd apps/ui
npm install
npm run dev
```
