# Doppler Revision De-duplication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop CLS Doppler location *revisions* from creating duplicate, time-shifted EarthRanger observations by emitting each fix once, at its final revision.

**Architecture:** A new pure function `collapse_doppler_revisions` runs after the existing transform, over the produced observation list. It (1) holds Doppler observations newer than a configurable settle window and (2) collapses observations sharing a `(source, dopplerLocId)` to the highest `dopplerRevision`. Both pull and backfill handlers call it before batching to Gundi. No cross-run state — once a fix is past the settle window its `recorded_at` is stable, so later re-sends are dropped by ER's first-write-wins dedup.

**Tech Stack:** Python 3.10, pydantic ~1.10, pytest, pytest-asyncio, pytest-mock.

**Spec:** `docs/superpowers/specs/2026-06-07-doppler-revision-dedup-design.md`

---

## File Structure

- **Modify** `app/actions/transformers.py` — add `collapse_doppler_revisions` and two private helpers (`_parse_iso_utc`, `_revision_sort_key`). Pure, no I/O.
- **Modify** `app/actions/configurations.py` — add `doppler_settle_hours` to `PullTelemetryConfiguration` and `BackfillTelemetryConfiguration`.
- **Modify** `app/actions/handlers.py` — call the collapse function in `action_pull_telemetry` and `action_backfill_telemetry`; add stats to the summary log.
- **Create** `app/services/tests/test_doppler_revision_collapse.py` — unit tests for the pure function.
- **Modify** `app/services/tests/test_kineis_action.py` — handler-level test asserting collapse is applied.

---

## Task 1: `collapse_doppler_revisions` pure function

**Files:**
- Create: `app/services/tests/test_doppler_revision_collapse.py`
- Modify: `app/actions/transformers.py`

- [ ] **Step 1: Write the failing tests**

Create `app/services/tests/test_doppler_revision_collapse.py`:

```python
"""Tests for Doppler revision de-duplication (ERCS-7275)."""

from datetime import datetime, timedelta, timezone

from app.actions.transformers import (
    collapse_doppler_revisions,
    telemetry_to_observation,
)

NOW = datetime(2026, 5, 18, 0, 0, 0, tzinfo=timezone.utc)  # day after the fixes
SETTLE = timedelta(hours=6)


def _doppler_msg(device_ref, loc_id, revision, dt, lat, lon, cls="2", err=300.0, acq=None):
    return {
        "deviceRef": device_ref,
        "dopplerLocId": loc_id,
        "dopplerRevision": revision,
        "dopplerDatetime": dt,
        "dopplerAcqDatetime": acq or dt,
        "dopplerLocLat": lat,
        "dopplerLocLon": lon,
        "dopplerLocClass": cls,
        "dopplerLocErrorRadius": err,
        "msgType": "FORMAT TEMPL. 31X8-A1 #1",
    }


def _obs(device_ref, loc_id, revision, dt, lat, lon, **kw):
    obs = telemetry_to_observation(_doppler_msg(device_ref, loc_id, revision, dt, lat, lon, **kw))
    assert obs is not None and obs["location_type"] == "doppler"
    return obs


def test_collapses_revisions_of_same_locid_to_highest():
    obs = [
        _obs("45020", 11, 0, "2026-05-17T01:28:53.197", -46.60373, 168.33551, cls="B"),
        _obs("45020", 11, 2, "2026-05-17T01:31:09.327", -46.68636, 168.31857, cls="2"),
    ]
    kept, stats = collapse_doppler_revisions(obs, SETTLE, NOW)
    assert len(kept) == 1
    assert kept[0]["additional"]["dopplerRevision"] == 2
    assert kept[0]["location"]["lat"] == -46.68636
    assert stats["revisions_collapsed"] == 1
    assert stats["held_unsettled"] == 0


def test_roxy_locid_12_keeps_offshore_rev2():
    obs = [
        _obs("45020", 12, 1, "2026-05-17T01:53:40.901", -46.6201, 168.34167, cls="B"),
        _obs("45020", 12, 2, "2026-05-17T01:55:11.781", -46.73484, 168.30432, cls="2"),
    ]
    kept, stats = collapse_doppler_revisions(obs, SETTLE, NOW)
    assert len(kept) == 1
    assert kept[0]["location"]["lat"] == -46.73484
    assert stats["revisions_collapsed"] == 1


def test_holds_unsettled_doppler():
    recent = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "")
    obs = [_obs("45020", 99, 0, recent, -46.7, 168.3)]
    kept, stats = collapse_doppler_revisions(obs, SETTLE, NOW)
    assert kept == []
    assert stats["held_unsettled"] == 1
    assert stats["revisions_collapsed"] == 0


def test_settle_zero_disables_holding():
    recent = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "")
    obs = [_obs("45020", 99, 0, recent, -46.7, 168.3)]
    kept, stats = collapse_doppler_revisions(obs, timedelta(0), NOW)
    assert len(kept) == 1
    assert stats["held_unsettled"] == 0


def test_passes_through_non_doppler_and_distinct_fixes():
    gps = telemetry_to_observation({
        "deviceRef": "D1",
        "recordedAt": "2026-05-17T01:00:00.000Z",
        "gpsLocDatetime": "2026-05-17T01:00:00.000Z",
        "gpsLocLat": -46.5,
        "gpsLocLon": 168.0,
    })
    assert gps is not None and gps["location_type"] == "gps"
    d1 = _obs("45020", 9, 0, "2026-05-17T01:10:20.045", -46.60374, 168.33552, cls="3")
    d2 = _obs("45020", 10, 0, "2026-05-17T01:20:38.412", -46.60373, 168.33551, cls="B")
    kept, stats = collapse_doppler_revisions([gps, d1, d2], SETTLE, NOW)
    assert len(kept) == 3
    assert stats["revisions_collapsed"] == 0


def test_missing_locid_never_collapses():
    a = _obs("45020", None, 0, "2026-05-17T01:10:20.045", -46.6, 168.3)
    b = _obs("45020", None, 0, "2026-05-17T01:20:20.045", -46.7, 168.4)
    kept, stats = collapse_doppler_revisions([a, b], SETTLE, NOW)
    assert len(kept) == 2
    assert stats["revisions_collapsed"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/services/tests/test_doppler_revision_collapse.py -v`
Expected: FAIL with `ImportError: cannot import name 'collapse_doppler_revisions'`.

- [ ] **Step 3: Implement the function**

In `app/actions/transformers.py`, the import line is currently:

```python
from datetime import datetime, timezone
```

Change it to include `timedelta`:

```python
from datetime import datetime, timedelta, timezone
```

Then append to the end of `app/actions/transformers.py`:

```python
def _parse_iso_utc(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 string (optionally trailing 'Z') to an aware UTC datetime."""
    if not value:
        return None
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _revision_sort_key(obs: Dict[str, Any], idx: int) -> Tuple[int, float, int]:
    """Sort key for picking the winning revision: highest revision, then latest
    acquisition time, then last seen (idx) for determinism."""
    add = obs.get("additional") or {}
    try:
        revision = int(add.get("dopplerRevision"))
    except (TypeError, ValueError):
        revision = -1
    acq = _parse_iso_utc(add.get("dopplerAcqDatetime")) or _parse_iso_utc(obs.get("recorded_at"))
    acq_ts = acq.timestamp() if acq else float("-inf")
    return (revision, acq_ts, idx)


def collapse_doppler_revisions(
    observations: List[Dict[str, Any]],
    settle_window: timedelta,
    now: datetime,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Collapse CLS Doppler location revisions to one observation per fix.

    Only observations with location_type == "doppler" are affected:
      1. Settle: drop (hold) any whose recorded_at is newer than now - settle_window.
         Skipped when settle_window <= 0. Held observations are re-fetched and
         emitted by a later run once past the window.
      2. Collapse: group by (source, dopplerLocId) and keep only the highest
         dopplerRevision (tie-break: latest dopplerAcqDatetime, then last seen).
         Observations with no dopplerLocId each form their own group.

    Non-doppler observations pass through untouched.

    Returns (kept_observations, stats) where stats has integer keys
    "held_unsettled" and "revisions_collapsed".
    """
    passthrough: List[Dict[str, Any]] = []
    doppler: List[Dict[str, Any]] = []
    for obs in observations:
        (doppler if obs.get("location_type") == "doppler" else passthrough).append(obs)

    held = 0
    if settle_window and settle_window > timedelta(0):
        cutoff = now - settle_window
        settled: List[Dict[str, Any]] = []
        for obs in doppler:
            recorded = _parse_iso_utc(obs.get("recorded_at"))
            if recorded is not None and recorded > cutoff:
                held += 1
            else:
                settled.append(obs)
        doppler = settled

    groups: Dict[Any, List[Tuple[int, Dict[str, Any]]]] = {}
    order: List[Any] = []
    for idx, obs in enumerate(doppler):
        loc_id = (obs.get("additional") or {}).get("dopplerLocId")
        key = ("__no_locid__", idx) if loc_id is None else (obs.get("source"), loc_id)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append((idx, obs))

    collapsed = 0
    kept_doppler: List[Dict[str, Any]] = []
    for key in order:
        members = groups[key]
        best = max(members, key=lambda m: _revision_sort_key(m[1], m[0]))
        kept_doppler.append(best[1])
        collapsed += len(members) - 1

    return passthrough + kept_doppler, {"held_unsettled": held, "revisions_collapsed": collapsed}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest app/services/tests/test_doppler_revision_collapse.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/transformers.py app/services/tests/test_doppler_revision_collapse.py
git commit -m "feat: add collapse_doppler_revisions for CLS revision de-dup (ERCS-7275)"
```

---

## Task 2: Add `doppler_settle_hours` configuration

**Files:**
- Modify: `app/actions/configurations.py`
- Modify: `app/services/tests/test_kineis_action.py` (add a config test)

- [ ] **Step 1: Write the failing test**

Add to `app/services/tests/test_kineis_action.py` (after the existing `test_pull_telemetry_config_rejects_both_device_refs_and_uids`):

```python
def test_pull_telemetry_config_doppler_settle_hours_default_and_bounds():
    """doppler_settle_hours defaults to 6 and is bounded 0..48."""
    cfg = PullTelemetryConfiguration(lookback_hours=4, page_size=100, use_realtime=False)
    assert cfg.doppler_settle_hours == 6

    with pytest.raises(pydantic.ValidationError):
        PullTelemetryConfiguration(lookback_hours=4, page_size=100, doppler_settle_hours=-1)
    with pytest.raises(pydantic.ValidationError):
        PullTelemetryConfiguration(lookback_hours=4, page_size=100, doppler_settle_hours=49)

    bf = BackfillTelemetryConfiguration(lookback_hours=24, page_size=100)
    assert bf.doppler_settle_hours == 6
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest app/services/tests/test_kineis_action.py::test_pull_telemetry_config_doppler_settle_hours_default_and_bounds -v`
Expected: FAIL with `AttributeError`/`ValidationError` (field does not exist).

- [ ] **Step 3: Add the config field to both classes**

In `app/actions/configurations.py`, inside `PullTelemetryConfiguration`, add this field immediately after the `use_realtime` field (before the `@root_validator`):

```python
    doppler_settle_hours: int = FieldWithUIOptions(
        6,
        ge=0,
        le=48,
        title="Doppler settle hours",
        description="Hold Doppler locations until they are at least this many hours old, so CLS revisions finalize before sending. 0 disables holding (collapse still applies within a batch).",
        ui_options=UIOptions(
            widget="range",  # slider
        ),
    )
```

In the same class, add `"doppler_settle_hours"` to `ui_global_options.order` right after `"use_realtime"`:

```python
    ui_global_options = GlobalUISchemaOptions(
        order=[
            "lookback_hours",
            "page_size",
            "use_realtime",
            "doppler_settle_hours",
            "device_refs",
            "device_uids",
            "retrieve_metadata",
            "retrieve_raw_data",
        ],
    )
```

In `BackfillTelemetryConfiguration`, add the identical field immediately after the `page_size` field (before `device_refs`):

```python
    doppler_settle_hours: int = FieldWithUIOptions(
        6,
        ge=0,
        le=48,
        title="Doppler settle hours",
        description="Hold Doppler locations until they are at least this many hours old, so CLS revisions finalize before sending. 0 disables holding (collapse still applies within a batch).",
        ui_options=UIOptions(widget="range"),
    )
```

And add `"doppler_settle_hours"` to that class's `ui_global_options.order` right after `"page_size"`:

```python
    ui_global_options = GlobalUISchemaOptions(
        order=[
            "lookback_hours",
            "page_size",
            "doppler_settle_hours",
            "device_refs",
            "device_uids",
            "retrieve_metadata",
            "retrieve_raw_data",
        ],
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest app/services/tests/test_kineis_action.py::test_pull_telemetry_config_doppler_settle_hours_default_and_bounds -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/actions/configurations.py app/services/tests/test_kineis_action.py
git commit -m "feat: add doppler_settle_hours config to pull/backfill actions"
```

---

## Task 3: Wire collapse into both handlers + logging

**Files:**
- Modify: `app/actions/handlers.py`
- Modify: `app/services/tests/test_kineis_action.py`

- [ ] **Step 1: Write the failing test**

Add to `app/services/tests/test_kineis_action.py`:

```python
@pytest.mark.asyncio
async def test_action_pull_telemetry_collapses_doppler_revisions(
    mocker, integration_with_id, authenticate_kineis_config
):
    """Two revisions of one dopplerLocId collapse to a single observation (highest revision)."""
    config = PullTelemetryConfiguration(
        lookback_hours=4, page_size=100, use_realtime=False, doppler_settle_hours=0,
    )
    messages = [
        {
            "deviceRef": "45020", "dopplerLocId": 11, "dopplerRevision": 0,
            "dopplerDatetime": "2026-05-17T01:28:53.197",
            "dopplerLocLat": -46.60373, "dopplerLocLon": 168.33551, "dopplerLocClass": "B",
        },
        {
            "deviceRef": "45020", "dopplerLocId": 11, "dopplerRevision": 2,
            "dopplerDatetime": "2026-05-17T01:31:09.327",
            "dopplerLocLat": -46.68636, "dopplerLocLon": 168.31857, "dopplerLocClass": "2",
        },
    ]
    mock_send = AsyncMock(return_value={})
    mocker.patch("app.actions.handlers.fetch_telemetry", AsyncMock(return_value=messages))
    mocker.patch("app.actions.handlers.fetch_device_list", AsyncMock(return_value=[]))
    mocker.patch("app.actions.handlers.send_observations_to_gundi", mock_send)
    mocker.patch("app.actions.handlers.log_action_activity", AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    mocker.patch("app.actions.handlers.get_auth_config", return_value=authenticate_kineis_config)

    result = await action_pull_telemetry(
        integration=integration_with_id, action_config=config,
    )

    assert result["messages_fetched"] == 2
    assert result["observations_sent"] == 1
    sent = mock_send.call_args[1]["observations"]
    assert len(sent) == 1
    assert sent[0]["additional"]["dopplerRevision"] == 2
    assert sent[0]["location"]["lat"] == -46.68636
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest app/services/tests/test_kineis_action.py::test_action_pull_telemetry_collapses_doppler_revisions -v`
Expected: FAIL — `observations_sent == 2` (both revisions still sent).

- [ ] **Step 3: Update the import in handlers.py**

In `app/actions/handlers.py`, the import is currently:

```python
from app.actions.transformers import telemetry_batch_to_observations_detailed
```

Change it to:

```python
from app.actions.transformers import (
    collapse_doppler_revisions,
    telemetry_batch_to_observations_detailed,
)
```

- [ ] **Step 4: Apply collapse in `action_pull_telemetry`**

In `app/actions/handlers.py`, within `action_pull_telemetry`, find:

```python
    transform_result = telemetry_batch_to_observations_detailed(
        messages,
        device_uid_to_customer_name=device_uid_to_customer_name,
    )
    observations = transform_result.observations
    skipped = transform_result.total_skipped
```

Replace the last two lines so it reads:

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

Then, in the same function, find the summary-building block:

```python
    if skipped:
        summary_data["skipped"] = skipped
        summary_data["skip_reasons"] = transform_result.skip_reasons
```

and add immediately after it:

```python
    if dedup_stats["revisions_collapsed"]:
        summary_data["doppler_revisions_collapsed"] = dedup_stats["revisions_collapsed"]
    if dedup_stats["held_unsettled"]:
        summary_data["doppler_held_unsettled"] = dedup_stats["held_unsettled"]
```

- [ ] **Step 5: Apply the same change in `action_backfill_telemetry`**

In `action_backfill_telemetry`, find:

```python
    transform_result = telemetry_batch_to_observations_detailed(
        messages,
        device_uid_to_customer_name=device_uid_to_customer_name,
    )
    observations = transform_result.observations
    skipped = transform_result.total_skipped
```

Replace the last two lines so it reads:

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

Then find the backfill summary block:

```python
    if skipped:
        summary_data["skipped"] = skipped
        summary_data["skip_reasons"] = transform_result.skip_reasons
```

and add immediately after it:

```python
    if dedup_stats["revisions_collapsed"]:
        summary_data["doppler_revisions_collapsed"] = dedup_stats["revisions_collapsed"]
    if dedup_stats["held_unsettled"]:
        summary_data["doppler_held_unsettled"] = dedup_stats["held_unsettled"]
```

- [ ] **Step 6: Run the new test and the full action suite**

Run: `pytest app/services/tests/test_kineis_action.py -v`
Expected: PASS, including `test_action_pull_telemetry_collapses_doppler_revisions` and all pre-existing action tests (the GPS-based samples are untouched because the settle/collapse steps only act on `location_type == "doppler"`).

- [ ] **Step 7: Run the full Kineis suite to confirm no regressions**

Run: `pytest app/services/tests/test_kineis_transformers.py app/services/tests/test_kineis_action.py app/services/tests/test_doppler_revision_collapse.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add app/actions/handlers.py app/services/tests/test_kineis_action.py
git commit -m "feat: apply Doppler revision collapse in pull/backfill handlers (ERCS-7275)"
```

---

## Self-Review

**1. Spec coverage**
- Settle filter → Task 1 (`collapse_doppler_revisions`, settle step) + Task 3 (wiring with `doppler_settle_hours`). ✓
- Revision collapse by `(source, dopplerLocId)`, max revision, deterministic tie-break → Task 1 (`_revision_sort_key`, grouping). ✓
- GPS / unlocated pass through → Task 1 (`test_passes_through_non_doppler_and_distinct_fixes`). ✓
- Missing `dopplerLocId` never collapses → Task 1 (`test_missing_locid_never_collapses`). ✓
- `doppler_settle_hours` config (default 6, range 0–48, both actions, UI order) → Task 2. ✓
- Handler wiring in both pull and backfill → Task 3 (Steps 4–5). ✓
- Logging `doppler_held_unsettled` / `doppler_revisions_collapsed` → Task 3 (Steps 4–5). ✓
- Cross-run stability via stable `recorded_at` + ER dedup → no code needed; relies on the finalized `dopplerDatetime` and existing ER behavior. ✓
- Roxy regression (locId 11 rev0+rev2, locId 12 rev1+rev2) → Task 1 (`test_collapses_...`, `test_roxy_locid_12_...`). ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code. ✓

**3. Type consistency:** `collapse_doppler_revisions(observations, settle_window: timedelta, now: datetime) -> (list, dict)` is defined once in Task 1 and called with the same signature in Task 3. Stats keys `held_unsettled` / `revisions_collapsed` are produced in Task 1 and read in Task 3. `doppler_settle_hours` defined in Task 2 and read in Task 3. ✓
