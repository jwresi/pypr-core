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

## UI Setup

```bash
cd apps/ui
npm install
npm run dev
```
