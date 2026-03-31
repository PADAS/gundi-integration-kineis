# Implementation Plan: `action_backfill_telemetry`

## Problem

The realtime `pull_telemetry` action uses a checkpoint-based API. Kineis computes Doppler
locations asynchronously — a message arrives with no location, and the location is attached
hours later. By then, the checkpoint has moved past that message and it's never re-fetched,
creating silent position gaps in EarthRanger.

**Confirmed example:** device `45011` transmitted on March 23, but production never picked up
that data. A local bulk API query found it today.

---

## Proposed Solution

Add a new `action_backfill_telemetry` action that runs **once daily at 02:00 UTC** using the
bulk API with a 24-hour lookback. EarthRanger deduplication handles any overlap with data
already sent by the realtime action. Zero changes to the existing realtime action.

---

## Files to Change

| File | Change |
|---|---|
| `app/actions/configurations.py` | Add `BackfillTelemetryConfiguration` |
| `app/actions/handlers.py` | Add `action_backfill_telemetry` handler |
| `app/services/tests/test_kineis_action.py` | Add 3 new test cases |

---

## 1. `configurations.py` — New Config Class

Add `BackfillTelemetryConfiguration` after `PullTelemetryConfiguration`. No new imports needed.

```python
class BackfillTelemetryConfiguration(PullActionConfiguration):
    """
    Daily bulk backfill config. Re-fetches the last N hours via the bulk API
    to pick up messages whose Doppler locations were computed late.
    Credentials come from the Auth action.
    """
    lookback_hours: int = FieldWithUIOptions(
        24, ge=1, le=168,
        title="Lookback hours",
        description="Hours to look back. Default 24h; max 168h (7 days).",
        ui_options=UIOptions(widget="range"),
    )
    page_size: int = FieldWithUIOptions(
        100, ge=1, le=500,
        title="Page size",
        ui_options=UIOptions(widget="range"),
    )
    device_refs: Optional[List[str]] = FieldWithUIOptions(default=None, title="Device refs")
    device_uids: Optional[List[int]] = FieldWithUIOptions(default=None, title="Device UIDs")
    retrieve_metadata: bool = pydantic.Field(True)
    retrieve_raw_data: bool = pydantic.Field(True)

    @root_validator
    def device_filter_single(cls, values):
        if (values.get("device_refs") or []) and (values.get("device_uids") or []):
            raise ValueError("Provide only one of device_refs or device_uids.")
        return values

    ui_global_options = GlobalUISchemaOptions(
        order=["lookback_hours", "page_size", "device_refs", "device_uids",
               "retrieve_metadata", "retrieve_raw_data"],
    )
```

---

## 2. `handlers.py` — New Handler

Add to imports:
```python
from app.actions.configurations import (
    AuthenticateKineisConfig,
    BackfillTelemetryConfiguration,  # new
    PullTelemetryConfiguration,
    get_auth_config,
)
```

Add handler after `action_pull_telemetry`:

```python
@crontab_schedule("0 2 * * *")  # Daily at 02:00 UTC
@activity_logger()
async def action_backfill_telemetry(integration, action_config: BackfillTelemetryConfiguration):
    """
    Daily bulk backfill: re-fetch last N hours via bulk API to catch messages
    whose Doppler locations were computed after the realtime checkpoint advanced.
    EarthRanger deduplication handles overlap with realtime data.
    """
    integration_id = str(integration.id)
    action_id = "backfill_telemetry"
    auth_config = get_auth_config(integration)
    device_refs = action_config.device_refs or None
    device_uids = action_config.device_uids or None

    to_time = _utc_now()
    from_time = to_time - timedelta(hours=action_config.lookback_hours)
    from_str = _format_utc(from_time)
    to_str = _format_utc(to_time)

    await log_action_activity(
        integration_id=integration_id,
        action_id=action_id,
        title="Backfill: fetching telemetry from Kineis bulk API",
        level=LogLevel.INFO,
        data={"from": from_str, "to": to_str, "lookback_hours": action_config.lookback_hours},
    )

    messages = await fetch_telemetry(
        integration_id=integration_id,
        username=auth_config.username,
        password=auth_config.password.get_secret_value(),
        from_datetime=from_str,
        to_datetime=to_str,
        page_size=action_config.page_size,
        device_refs=device_refs,
        device_uids=device_uids,
        retrieve_metadata=action_config.retrieve_metadata,
        retrieve_raw_data=action_config.retrieve_raw_data,
        client_id=auth_config.client_id,
    )

    if not messages:
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title="Backfill: no telemetry messages found in window",
            level=LogLevel.INFO,
            data={"messages_fetched": 0, "from": from_str, "to": to_str},
        )
        return {"messages_fetched": 0, "observations_sent": 0, "skipped": 0}

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
        logger.warning("Backfill: could not fetch device list, using fallback: %s", e)

    transform_result = telemetry_batch_to_observations_detailed(
        messages,
        device_uid_to_customer_name=device_uid_to_customer_name,
    )
    observations = transform_result.observations
    skipped = transform_result.total_skipped

    sent_total = 0
    for batch in generate_batches(observations, OBSERVATION_BATCH_SIZE):
        await send_observations_to_gundi(
            observations=list(batch),
            integration_id=integration.id,
        )
        sent_total += len(batch)

    summary_data: Dict = {"messages_fetched": len(messages), "observations_sent": sent_total}
    if transform_result.msg_types_seen:
        summary_data["message_types"] = transform_result.msg_types_seen
    if skipped:
        summary_data["skipped"] = skipped
        summary_data["skip_reasons"] = transform_result.skip_reasons

    if sent_total > 0:
        title = (
            f"Backfill: sent {sent_total} observations to Gundi ({skipped} skipped)"
            if skipped else f"Backfill: sent {sent_total} observations to Gundi"
        )
        level = LogLevel.INFO
    else:
        title = f"Backfill: no observations could be created from {len(messages)} messages"
        level = LogLevel.WARNING

    await log_action_activity(
        integration_id=integration_id,
        action_id=action_id,
        title=title,
        level=level,
        data=summary_data,
    )

    return {"messages_fetched": len(messages), "observations_sent": sent_total, "skipped": skipped}
```

---

## 3. Tests — 3 New Cases

**Fixture:**
```python
@pytest.fixture
def backfill_telemetry_config():
    return BackfillTelemetryConfiguration(lookback_hours=24, page_size=100)
```

**Test 1 — Happy path:**
Verify bulk API is called with a time window, 2 messages fetched, 2 observations sent via
`send_observations_to_gundi`. Use Doppler coordinates to document the primary use case.

**Test 2 — No messages:**
Verify early return, `send_observations_to_gundi` not called, `fetch_device_list` not called.

**Test 3 — All skipped (no location):**
Verify `observations_sent=0`, `skipped=2`, `skip_reasons={"no_location": 2}`, WARNING log emitted.

---

## Notes for Reviewer

- **No changes to `action_pull_telemetry`** — fully additive
- `action_backfill_telemetry` uses no Redis state — no checkpoint to manage
- The `"Backfill:"` prefix in all log titles makes it easy to distinguish in the portal activity feed
- The `le=168` cap on `lookback_hours` matches Kineis bulk API documented retention; can be raised if needed
- Action discovery is automatic — `discover_actions()` finds all `action_` prefixed functions in `handlers.py`
