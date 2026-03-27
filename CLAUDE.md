# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Gundi v2 pull integration for **Kineis/CLS** satellite telemetry (Jira CONNECTORS-836). Pulls bulk and realtime telemetry from the CLS API and sends observations to Gundi/Earth Ranger.

- Python 3.10, async, FastAPI, pydantic ~1.10, httpx (async, pinned <0.28 for Starlette TestClient compatibility)
- Deployed on GCP (Cloud Run), triggered via PubSub messages
- Redis for state management (realtime checkpoint persistence)

## Commands

```bash
# Install dependencies (use pip-compile to resolve from .in files)
pip-compile --output-file=requirements.txt requirements-base.in requirements-dev.in requirements.in
pip install -r requirements.txt

# Run all tests
pytest

# Run Kineis-specific tests
pytest app/services/tests/test_kineis_client.py app/services/tests/test_kineis_transformers.py app/services/tests/test_kineis_action.py -v

# Run a single test
pytest app/services/tests/test_kineis_client.py::test_name -v

# Run dev server locally
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload

# Docker (from repo root)
docker compose -f docker/docker-compose.yml up
```

## Architecture

### Request flow

PubSub message → `POST /` (main.py) → base64-decodes payload → `execute_action()` dispatches to the handler in `app/actions/handlers.py` based on `action_id`.

### Key modules

- **`app/actions/handlers.py`** — Two actions: `action_auth` (credential verification) and `action_pull_telemetry` (scheduled every 2 min). Pull telemetry has two paths: realtime (checkpoint-based, default) and bulk (lookback window fallback).
- **`app/actions/configurations.py`** — Pydantic configs: `AuthenticateKineisConfig`, `PullTelemetryConfiguration`. `get_auth_config(integration)` extracts credentials from the integration's auth action config (not from env).
- **`app/actions/transformers.py`** — `telemetry_batch_to_observations_detailed()` maps CLS messages to Gundi observation dicts. Handles GPS (`gpsLocLat`/`gpsLocLon`) and Doppler (`dopplerLocLat`/`dopplerLocLon`) locations.
- **`app/services/kineis_client.py`** — Async CLS API client: token cache, `fetch_telemetry` (bulk with cursor pagination), `fetch_telemetry_realtime` (checkpoint-based), `fetch_device_list`. Single device list filter: `deviceRefs` **or** `deviceUids`, not both.
- **`app/services/state.py`** — `IntegrationStateManager` (Redis): get/set state per integration+action. Used for the realtime checkpoint (`kineis_realtime_checkpoint` key).
- **`app/datasource/kineis.py`** — Reference/example client (not used by action runner). Illustrates sync patterns.

### Framework (inherited, don't edit)

Files in `app/services/` (except `kineis_client.py`), `app/routers/`, `app/services/tests/test_action_runner.py`, etc. are shared framework code from the gundi-integration template. `requirements-base.in` and `requirements-dev.in` are framework-managed; add integration deps to `requirements.in`.

### Settings

- `app/settings/base.py` — Framework settings (Gundi API, Keycloak, Redis, GCP)
- `app/settings/integration.py` — Kineis-specific: `KINEIS_AUTH_BASE_URL`, `KINEIS_API_BASE_URL`, `KINEIS_AUTH_PATH` (all have defaults)
- Credentials are configured per integration in the Gundi portal, not via env vars

## Conventions

- **Realtime vs bulk:** Realtime (checkpoint in Redis) is default. If state unavailable or `use_realtime=False`, falls back to bulk with lookback window.
- **Device filter:** Only one of `device_refs` or `device_uids` per request (validated in config; client sends one).
- **Observations:** Batched in groups of 200 before sending to Gundi.
- **API alignment:** CLS API-Telemetry User Manual v1.2 — cursor pagination (`first`/`after`), retrieve-bulk and retrieve-realtime endpoints.
- **Testing:** All Kineis tests use mocked auth, fetch, and state. Tests are in `app/services/tests/test_kineis_*.py`.

## Reference docs

- `docs/kineis-api-reference.md` — Kineis API endpoints, auth roles, message types/fields
- `docs/kineis-api-samples/` — Sample request/response JSON payloads for bulk and realtime endpoints
