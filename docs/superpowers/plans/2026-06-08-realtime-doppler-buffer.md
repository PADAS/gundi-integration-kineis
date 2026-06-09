# Realtime Doppler Held-Fix Buffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the realtime pull path re-emit Doppler fixes once they settle (≈ settle window + one realtime interval) instead of waiting for the daily backfill, via a small persistent held-fix buffer in Redis.

**Architecture:** Add a pure function `reconcile_doppler_buffer(buffer, observations, settle_window, now)` that merges newly-fetched Doppler fixes into a per-`(source, dopplerLocId)` buffer (keeping the highest revision) and emits any fix past the settle window. The realtime branch of `action_pull_telemetry` loads the buffer from the existing Redis state, runs the reconciler, sends the emitted observations, and persists checkpoint+buffer together. The bulk-fallback path and the daily `backfill_telemetry` action keep using `collapse_doppler_revisions` unchanged.

**Tech Stack:** Python 3.10, pydantic ~1.10, pytest, pytest-asyncio, pytest-mock.

**Spec:** `docs/superpowers/specs/2026-06-08-realtime-doppler-buffer-design.md`

**Branch:** `realtime-doppler-buffer` (already checked out; the spec commit is its tip).

---

## File Structure

- **Modify** `app/actions/transformers.py` — add `reconcile_doppler_buffer` (pure). Reuses existing `_parse_iso_utc` and `_revision_sort_key`.
- **Modify** `app/actions/handlers.py` — wire the reconciler into the realtime branch of `action_pull_telemetry`; load/persist the buffer; branch realtime vs bulk; drain the buffer even on no-new-message runs; logging.
- **Create** `app/services/tests/test_doppler_buffer.py` — unit tests for `reconcile_doppler_buffer`.
- **Modify** `app/services/tests/test_kineis_action.py` — update the existing realtime test for the new `set_state` payload; add a two-run buffer→emit handler test.

---

## Task 1: `reconcile_doppler_buffer` pure function

**Files:**
- Create: `app/services/tests/test_doppler_buffer.py`
- Modify: `app/actions/transformers.py`

- [ ] **Step 1: Write the failing tests**

Create `app/services/tests/test_doppler_buffer.py`:

```python
"""Tests for the realtime Doppler held-fix buffer (ERCS-7275 follow-up)."""

from datetime import datetime, timedelta, timezone

import pytest

from app.actions.transformers import reconcile_doppler_buffer, telemetry_to_observation

NOW = datetime(2026, 5, 18, 0, 0, 0, tzinfo=timezone.utc)  # day after the fixes
SETTLE = timedelta(hours=6)


def _doppler_msg(device_ref, loc_id, revision, dt, lat, lon, cls="2", acq=None):
    return {
        "deviceRef": device_ref,
        "dopplerLocId": loc_id,
        "dopplerRevision": revision,
        "dopplerDatetime": dt,
        "dopplerAcqDatetime": acq or dt,
        "dopplerLocLat": lat,
        "dopplerLocLon": lon,
        "dopplerLocClass": cls,
        "dopplerLocErrorRadius": 300.0,
        "msgType": "FORMAT TEMPL. 31X8-A1 #1",
    }


def _obs(device_ref, loc_id, revision, dt, lat, lon, **kw):
    obs = telemetry_to_observation(_doppler_msg(device_ref, loc_id, revision, dt, lat, lon, **kw))
    assert obs is not None and obs["location_type"] == "doppler"
    return obs


def _key(source, loc_id):
    return f"{source}|{loc_id}"


def test_unsettled_fix_is_buffered_not_emitted():
    recent = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "")
    obs = _obs("45020", 11, 0, recent, -46.6, 168.3)
    emit, new_buffer, stats = reconcile_doppler_buffer({}, [obs], SETTLE, NOW)
    assert emit == []
    assert _key("45020", 11) in new_buffer
    assert stats == {"buffered": 1, "emitted_from_buffer": 0, "revisions_collapsed": 0}


def test_buffered_fix_emitted_once_settled():
    # Buffer holds a fix recorded at 01:55; with now well past the window it settles.
    held = _obs("45020", 12, 2, "2026-05-17T01:55:11.781", -46.73484, 168.30432)
    buffer = {_key("45020", 12): held}
    emit, new_buffer, stats = reconcile_doppler_buffer(buffer, [], SETTLE, NOW)
    assert len(emit) == 1 and emit[0]["location"]["lat"] == -46.73484
    assert new_buffer == {}
    assert stats["emitted_from_buffer"] == 1
    assert stats["buffered"] == 0


def test_higher_revision_supersedes_buffered_fix():
    recent0 = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "")
    recent2 = (NOW - timedelta(minutes=58)).isoformat().replace("+00:00", "")
    rev0 = _obs("45020", 11, 0, recent0, -46.60, 168.33, cls="B")
    rev2 = _obs("45020", 11, 2, recent2, -46.68, 168.31, cls="2")
    buffer = {_key("45020", 11): rev0}
    emit, new_buffer, stats = reconcile_doppler_buffer(buffer, [rev2], SETTLE, NOW)
    assert emit == []  # still unsettled
    assert new_buffer[_key("45020", 11)]["additional"]["dopplerRevision"] == 2
    assert stats["revisions_collapsed"] == 1
    assert stats["buffered"] == 1


def test_gps_passes_through_immediately():
    gps = telemetry_to_observation({
        "deviceRef": "D1", "gpsLocDatetime": "2026-05-17T01:00:00.000Z",
        "gpsLocLat": -46.5, "gpsLocLon": 168.0,
    })
    assert gps is not None and gps["location_type"] == "gps"
    emit, new_buffer, stats = reconcile_doppler_buffer({}, [gps], SETTLE, NOW)
    assert emit == [gps]
    assert new_buffer == {}
    assert stats["buffered"] == 0


def test_settled_on_arrival_emitted_immediately():
    obs = _obs("45020", 9, 0, "2026-05-17T01:10:20.045", -46.60, 168.33)  # old vs NOW -> settled
    emit, new_buffer, stats = reconcile_doppler_buffer({}, [obs], SETTLE, NOW)
    assert len(emit) == 1
    assert new_buffer == {}
    assert stats["emitted_from_buffer"] == 1


def test_missing_locid_unsettled_dropped_settled_emitted():
    recent = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "")
    unsettled = _obs("45020", None, 0, recent, -46.7, 168.3)
    settled = _obs("45020", None, 0, "2026-05-17T01:10:20.045", -46.6, 168.2)
    emit, new_buffer, stats = reconcile_doppler_buffer({}, [unsettled, settled], SETTLE, NOW)
    assert settled in emit and unsettled not in emit
    assert new_buffer == {}  # never buffered (no locId)


def test_settle_zero_emits_collapsed_max_revision():
    recent0 = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "")
    recent2 = (NOW - timedelta(minutes=58)).isoformat().replace("+00:00", "")
    rev0 = _obs("45020", 11, 0, recent0, -46.60, 168.33, cls="B")
    rev2 = _obs("45020", 11, 2, recent2, -46.68, 168.31, cls="2")
    emit, new_buffer, stats = reconcile_doppler_buffer({}, [rev0, rev2], timedelta(0), NOW)
    assert len(emit) == 1 and emit[0]["additional"]["dopplerRevision"] == 2
    assert new_buffer == {}
    assert stats["revisions_collapsed"] == 1


def test_naive_now_raises_when_settle_active():
    recent = "2026-05-17T23:00:00"
    obs = _obs("45020", 11, 0, recent, -46.6, 168.3)
    with pytest.raises(ValueError):
        reconcile_doppler_buffer({}, [obs], SETTLE, datetime(2026, 5, 18, 0, 0, 0))  # naive
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/services/tests/test_doppler_buffer.py -v`
Expected: FAIL with `ImportError: cannot import name 'reconcile_doppler_buffer'`.

- [ ] **Step 3: Implement the function**

Append to the end of `app/actions/transformers.py` (it already imports `datetime, timedelta, timezone` and `Any, Dict, List, Optional, Tuple`, and defines `_parse_iso_utc` and `_revision_sort_key`):

```python
def reconcile_doppler_buffer(
    buffer: Dict[str, Dict[str, Any]],
    observations: List[Dict[str, Any]],
    settle_window: timedelta,
    now: datetime,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, int]]:
    """
    Merge new observations into a persistent held-fix buffer and emit settled fixes.

    Used by the realtime pull path so Doppler fixes are re-emitted once they settle
    rather than only by the daily backfill.

    - buffer: {"<source>|<dopplerLocId>": observation} held from previous runs.
    - Non-doppler observations are emitted immediately, never buffered.
    - Doppler observations with a dopplerLocId are upserted into the buffer by
      (source, dopplerLocId), keeping the higher dopplerRevision (tie-break: latest
      dopplerAcqDatetime, then last seen). A discarded lower revision counts as a
      collapsed revision.
    - Doppler observations with no dopplerLocId cannot be re-identified across runs:
      emitted if already settled, otherwise dropped (the daily backfill catches them).
    - A buffered fix is emitted and evicted once its kept recorded_at <= now - settle_window.
    - settle_window <= 0 disables holding: every fix is emitted (collapsed to max revision),
      buffer ends empty.

    Returns (emit, new_buffer, stats) where stats has integer keys
    "buffered", "emitted_from_buffer", "revisions_collapsed".
    now must be a timezone-aware UTC datetime when settle_window > 0.
    """
    cutoff: Optional[datetime] = None
    if settle_window > timedelta(0):
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware (e.g. datetime.now(timezone.utc))")
        cutoff = now - settle_window

    def is_settled(obs: Dict[str, Any]) -> bool:
        if cutoff is None:
            return True
        recorded = _parse_iso_utc(obs.get("recorded_at"))
        return recorded is None or recorded <= cutoff

    emit: List[Dict[str, Any]] = []
    working: Dict[str, Dict[str, Any]] = dict(buffer)
    collapsed = 0

    for obs in observations:
        if obs.get("location_type") != "doppler":
            emit.append(obs)
            continue
        loc_id = (obs.get("additional") or {}).get("dopplerLocId")
        if loc_id is None:
            if is_settled(obs):
                emit.append(obs)
            # else: cannot buffer without a stable key; backfill will re-emit it
            continue
        key = f"{obs.get('source')}|{loc_id}"
        existing = working.get(key)
        if existing is None:
            working[key] = obs
        else:
            best = max((existing, obs), key=lambda o: _revision_sort_key(o, 0))
            working[key] = best
            collapsed += 1

    new_buffer: Dict[str, Dict[str, Any]] = {}
    emitted_from_buffer = 0
    for key, obs in working.items():
        if is_settled(obs):
            emit.append(obs)
            emitted_from_buffer += 1
        else:
            new_buffer[key] = obs

    return emit, new_buffer, {
        "buffered": len(new_buffer),
        "emitted_from_buffer": emitted_from_buffer,
        "revisions_collapsed": collapsed,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest app/services/tests/test_doppler_buffer.py -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/transformers.py app/services/tests/test_doppler_buffer.py
git commit -m "feat: add reconcile_doppler_buffer for realtime held-fix re-emit (ERCS-7275)"
```

---

## Task 2: Wire the buffer into the realtime handler

**Files:**
- Modify: `app/actions/handlers.py`
- Modify: `app/services/tests/test_kineis_action.py`

- [ ] **Step 1: Update the existing realtime test for the new state payload + add the two-run buffer test**

In `app/services/tests/test_kineis_action.py`, find this assertion in
`test_action_pull_telemetry_uses_realtime_when_checkpoint_stored`:

```python
    assert call_state[2] == {"kineis_realtime_checkpoint": 12345}
```

Replace it with (the GPS sample message is emitted immediately, so the buffer is empty):

```python
    assert call_state[2] == {"kineis_realtime_checkpoint": 12345, "kineis_doppler_buffer": {}}
```

Then append this new test at the end of the file:

```python
@pytest.mark.asyncio
async def test_realtime_buffers_then_emits_settled_fix(
    mocker, integration_with_id, authenticate_kineis_config
):
    """A Doppler fix is buffered while unsettled (run 1) and emitted once settled (run 2)."""
    from datetime import datetime, timezone

    config = PullTelemetryConfiguration(
        lookback_hours=4, page_size=100, use_realtime=True, doppler_settle_hours=6,
    )

    # A fake Redis-backed state store shared across the two runs.
    store = {}

    async def fake_get_state(integration_id, action_id, source_id="no-source"):
        return dict(store)

    async def fake_set_state(integration_id, action_id, state, source_id="no-source"):
        store.clear()
        store.update(state)

    state_mgr = MagicMock()
    state_mgr.get_state = AsyncMock(side_effect=fake_get_state)
    state_mgr.set_state = AsyncMock(side_effect=fake_set_state)
    mocker.patch("app.actions.handlers.IntegrationStateManager", return_value=state_mgr)

    mock_send = AsyncMock(return_value={})
    mocker.patch("app.actions.handlers.fetch_telemetry", AsyncMock())
    mocker.patch("app.actions.handlers.fetch_device_list", AsyncMock(return_value=[]))
    mocker.patch("app.actions.handlers.send_observations_to_gundi", mock_send)
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mocker.patch("app.actions.handlers.get_auth_config", return_value=authenticate_kineis_config)

    fix_dt = "2026-05-17T01:55:00.000"
    msg = {
        "deviceRef": "45020", "dopplerLocId": 11, "dopplerRevision": 0,
        "dopplerDatetime": fix_dt, "dopplerLocLat": -46.7, "dopplerLocLon": 168.3,
        "dopplerLocClass": "2",
    }

    # Run 1: fix is ~5 min old relative to now -> within 6h window -> buffered, nothing sent.
    mocker.patch("app.actions.handlers.fetch_telemetry_realtime",
                 AsyncMock(return_value=([msg], 100)))
    mocker.patch("app.actions.handlers._utc_now",
                 return_value=datetime(2026, 5, 17, 2, 0, 0, tzinfo=timezone.utc))
    r1 = await action_pull_telemetry(integration=integration_with_id, action_config=config)
    assert r1["observations_sent"] == 0
    assert "45020|11" in store["kineis_doppler_buffer"]

    # Run 2: no new messages, but now is past the settle window -> buffered fix emitted.
    mocker.patch("app.actions.handlers.fetch_telemetry_realtime",
                 AsyncMock(return_value=([], 101)))
    mocker.patch("app.actions.handlers._utc_now",
                 return_value=datetime(2026, 5, 17, 9, 0, 0, tzinfo=timezone.utc))
    r2 = await action_pull_telemetry(integration=integration_with_id, action_config=config)
    assert r2["observations_sent"] == 1
    sent = mock_send.call_args[1]["observations"]
    assert sent[0]["additional"]["dopplerLocId"] == 11
    assert store["kineis_doppler_buffer"] == {}
    assert store["kineis_realtime_checkpoint"] == 101
```

- [ ] **Step 2: Run the new/updated tests to verify they fail**

Run: `pytest app/services/tests/test_kineis_action.py::test_realtime_buffers_then_emits_settled_fix app/services/tests/test_kineis_action.py::test_action_pull_telemetry_uses_realtime_when_checkpoint_stored -v`
Expected: FAIL — buffer not implemented in the handler yet (run 1 sends the fix instead of buffering; `kineis_doppler_buffer` missing from state).

- [ ] **Step 3: Add the import and state-key constant in `handlers.py`**

Change the import:

```python
from app.actions.transformers import telemetry_batch_to_observations_detailed
```
…and any existing multi-line variant that imports `collapse_doppler_revisions`, to:

```python
from app.actions.transformers import (
    collapse_doppler_revisions,
    reconcile_doppler_buffer,
    telemetry_batch_to_observations_detailed,
)
```

Add the buffer state key next to the existing checkpoint key:

```python
REALTIME_STATE_KEY = "kineis_realtime_checkpoint"
DOPPLER_BUFFER_STATE_KEY = "kineis_doppler_buffer"
```

- [ ] **Step 4: Load the buffer and stop persisting the checkpoint early**

In `action_pull_telemetry`, replace this block:

```python
    use_realtime = action_config.use_realtime
    checkpoint = 0
    if use_realtime:
        try:
            state_mgr = IntegrationStateManager()
            state = await state_mgr.get_state(integration_id, action_id)
            checkpoint = state.get(REALTIME_STATE_KEY, 0)
        except Exception as e:
            logger.warning("Could not load realtime checkpoint, using bulk: %s", e)
            use_realtime = False
```

with:

```python
    use_realtime = action_config.use_realtime
    checkpoint = 0
    doppler_buffer: Dict[str, Dict] = {}
    if use_realtime:
        try:
            state_mgr = IntegrationStateManager()
            state = await state_mgr.get_state(integration_id, action_id)
            checkpoint = state.get(REALTIME_STATE_KEY, 0)
            doppler_buffer = state.get(DOPPLER_BUFFER_STATE_KEY, {}) or {}
        except Exception as e:
            logger.warning("Could not load realtime checkpoint, using bulk: %s", e)
            use_realtime = False
```

Then delete the early checkpoint persistence (it moves to Step 7):

```python
        try:
            state_mgr = IntegrationStateManager()
            await state_mgr.set_state(
                integration_id, action_id, {REALTIME_STATE_KEY: new_checkpoint}
            )
        except Exception as e:
            logger.warning("Could not persist realtime checkpoint: %s", e)
```

(Remove those 7 lines entirely. `new_checkpoint` is still set by `fetch_telemetry_realtime` and used in Step 7.)

- [ ] **Step 5: Make the no-messages short-circuit realtime-buffer aware**

Replace this block:

```python
    if not messages:
        log_data = {"messages_fetched": 0}
        if use_realtime:
            log_data["checkpoint_from"] = checkpoint
            log_data["checkpoint_to"] = new_checkpoint
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title="No new telemetry messages from Kineis",
            level=LogLevel.INFO,
            data=log_data,
        )
        return {"messages_fetched": 0, "observations_sent": 0, "skipped": 0}
```

with (only short-circuit when there is also nothing buffered to drain; otherwise fall through so the buffer can release settled fixes):

```python
    if not messages and not (use_realtime and doppler_buffer):
        log_data = {"messages_fetched": 0}
        if use_realtime:
            log_data["checkpoint_from"] = checkpoint
            log_data["checkpoint_to"] = new_checkpoint
            try:
                state_mgr = IntegrationStateManager()
                await state_mgr.set_state(
                    integration_id, action_id,
                    {REALTIME_STATE_KEY: new_checkpoint, DOPPLER_BUFFER_STATE_KEY: doppler_buffer},
                )
            except Exception as e:
                logger.warning("Could not persist realtime checkpoint/buffer: %s", e)
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title="No new telemetry messages from Kineis",
            level=LogLevel.INFO,
            data=log_data,
        )
        return {"messages_fetched": 0, "observations_sent": 0, "skipped": 0}
```

- [ ] **Step 6: Only fetch the device list when there are new messages**

Replace:

```python
    device_uid_to_customer_name: Dict[int, str] = {}
    try:
        devices = await fetch_device_list(
            integration_id=integration_id,
            username=auth_config.username,
            password=auth_config.password.get_secret_value(),
            client_id=auth_config.client_id,
        )
        device_uid_to_customer_name = {
            d["deviceUid"]: d["customerName"]
            for d in devices
            if d.get("customerName")
        }
    except Exception as e:
        logger.warning("Could not fetch device list for source_name, using fallback: %s", e)
```

with (guard with `if messages:` so a buffer-drain-only run makes no needless API call):

```python
    device_uid_to_customer_name: Dict[int, str] = {}
    if messages:
        try:
            devices = await fetch_device_list(
                integration_id=integration_id,
                username=auth_config.username,
                password=auth_config.password.get_secret_value(),
                client_id=auth_config.client_id,
            )
            device_uid_to_customer_name = {
                d["deviceUid"]: d["customerName"]
                for d in devices
                if d.get("customerName")
            }
        except Exception as e:
            logger.warning("Could not fetch device list for source_name, using fallback: %s", e)
```

- [ ] **Step 7: Branch realtime (buffer) vs bulk (collapse); persist checkpoint+buffer**

Replace this block:

```python
    transform_result = telemetry_batch_to_observations_detailed(
        messages,
        device_uid_to_customer_name=device_uid_to_customer_name,
    )
    observations, dedup_stats = collapse_doppler_revisions(
        transform_result.observations,
        settle_window=timedelta(hours=action_config.doppler_settle_hours),
        now=_utc_now(),
    )
    skipped = transform_result.total_skipped
```

with:

```python
    transform_result = telemetry_batch_to_observations_detailed(
        messages,
        device_uid_to_customer_name=device_uid_to_customer_name,
    )
    settle = timedelta(hours=action_config.doppler_settle_hours)
    doppler_summary: Dict = {}
    if use_realtime:
        observations, doppler_buffer, buf_stats = reconcile_doppler_buffer(
            doppler_buffer, transform_result.observations, settle, _utc_now(),
        )
        if buf_stats["revisions_collapsed"]:
            doppler_summary["doppler_revisions_collapsed"] = buf_stats["revisions_collapsed"]
        if buf_stats["emitted_from_buffer"]:
            doppler_summary["doppler_emitted_from_buffer"] = buf_stats["emitted_from_buffer"]
        if buf_stats["buffered"]:
            doppler_summary["doppler_buffered"] = buf_stats["buffered"]
        try:
            state_mgr = IntegrationStateManager()
            await state_mgr.set_state(
                integration_id, action_id,
                {REALTIME_STATE_KEY: new_checkpoint, DOPPLER_BUFFER_STATE_KEY: doppler_buffer},
            )
        except Exception as e:
            logger.warning("Could not persist realtime checkpoint/buffer: %s", e)
    else:
        observations, dedup_stats = collapse_doppler_revisions(
            transform_result.observations, settle_window=settle, now=_utc_now(),
        )
        if dedup_stats["revisions_collapsed"]:
            doppler_summary["doppler_revisions_collapsed"] = dedup_stats["revisions_collapsed"]
        if dedup_stats["held_unsettled"]:
            doppler_summary["doppler_held_unsettled"] = dedup_stats["held_unsettled"]
    skipped = transform_result.total_skipped
```

- [ ] **Step 8: Replace the old dedup summary lines with the merged summary**

Find (in the summary-building section):

```python
    if dedup_stats["revisions_collapsed"]:
        summary_data["doppler_revisions_collapsed"] = dedup_stats["revisions_collapsed"]
    if dedup_stats["held_unsettled"]:
        summary_data["doppler_held_unsettled"] = dedup_stats["held_unsettled"]
```

Replace with:

```python
    summary_data.update(doppler_summary)
```

- [ ] **Step 9: Run the realtime + collapse + full action suites**

Run: `pytest app/services/tests/test_kineis_action.py -v`
Expected: PASS, including `test_realtime_buffers_then_emits_settled_fix`,
`test_action_pull_telemetry_uses_realtime_when_checkpoint_stored`, and all pre-existing tests
(bulk-path tests use `use_realtime=False` and still go through `collapse_doppler_revisions`).

- [ ] **Step 10: Run the full suite to confirm no regressions**

Run: `pytest -q`
Expected: PASS (all tests).

- [ ] **Step 11: Commit**

```bash
git add app/actions/handlers.py app/services/tests/test_kineis_action.py
git commit -m "feat: realtime path re-emits settled Doppler fixes via held-fix buffer (ERCS-7275)"
```

---

## Self-Review

**1. Spec coverage**
- Pure `reconcile_doppler_buffer(buffer, observations, settle_window, now)` with stated contract → Task 1. ✓
- Non-doppler emitted immediately; doppler upsert by (source, dopplerLocId) keeping max revision; missing-locId handling; emit-when-settled; settle<=0 empties buffer; naive-now ValueError → Task 1 tests + implementation. ✓
- Realtime branch loads buffer, reconciles, persists checkpoint+buffer together (replacing the early checkpoint-only persist) → Task 2 Steps 4, 7. ✓
- Drain buffer even when no new messages → Task 2 Step 5 (+ run-2 of the handler test). ✓
- Bulk fallback + daily backfill keep `collapse_doppler_revisions` → Task 2 Step 7 `else` branch; backfill handler untouched. ✓
- Logging `doppler_buffered` / `doppler_emitted_from_buffer` (+ `doppler_revisions_collapsed`) → Task 2 Steps 7–8. ✓
- Reuses `doppler_settle_hours`, no new config → Task 2 uses `action_config.doppler_settle_hours`. ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code. ✓

**3. Type consistency:** `reconcile_doppler_buffer` returns `(emit, new_buffer, stats)` in Task 1 and is destructured identically in Task 2 Step 7. Stats keys `buffered` / `emitted_from_buffer` / `revisions_collapsed` produced in Task 1 and read in Task 2. State keys `REALTIME_STATE_KEY` / `DOPPLER_BUFFER_STATE_KEY` defined in Task 2 Step 3 and used consistently in Steps 4, 5, 7. ✓
