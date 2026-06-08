"""
Kineis actions: auth (credential verification) and pull_telemetry (CONNECTORS-836).
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict

from app.actions.configurations import (
    AuthenticateKineisConfig,
    BackfillTelemetryConfiguration,
    PullTelemetryConfiguration,
    get_auth_config,
)
from app.actions.transformers import (
    collapse_doppler_revisions,
    reconcile_doppler_buffer,
    telemetry_batch_to_observations_detailed,
)
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_observations_to_gundi
from app.services.kineis_client import fetch_device_list, fetch_telemetry, fetch_telemetry_realtime, get_access_token
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches
from gundi_core.events import LogLevel

logger = logging.getLogger(__name__)

OBSERVATION_BATCH_SIZE = 200
REALTIME_STATE_KEY = "kineis_realtime_checkpoint"
DOPPLER_BUFFER_STATE_KEY = "kineis_doppler_buffer"


async def action_auth(integration, action_config: AuthenticateKineisConfig):
    """
    Verify Kineis/CLS credentials by obtaining a Bearer token.
    Used by the portal to validate credentials without running pull_telemetry.
    """
    try:
        result = await get_access_token(
            username=action_config.username,
            password=action_config.password.get_secret_value(),
            client_id=action_config.client_id,
        )
        return {"valid_credentials": True, "expires_in": result.get("expires_in")}
    except Exception as e:
        logger.exception("Kineis auth failed")
        return {"valid_credentials": False, "message": str(e)}


def _utc_now():
    return datetime.now(timezone.utc)


def _format_utc(dt: datetime) -> str:
    """Format datetime as YYYY-MM-DDTHH:mm:ss.SSSZ for Kineis API."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@crontab_schedule("*/2 * * * *")  # Every 2 minutes
@activity_logger()
async def action_pull_telemetry(integration, action_config: PullTelemetryConfiguration):
    """
    Pull telemetry from Kineis: when use_realtime is enabled and a checkpoint is stored,
    use the realtime API (only new data since last run). Otherwise use bulk API with
    lookback window. Map to Gundi observations and send. Credentials from Auth action config.
    """
    integration_id = str(integration.id)
    action_id = "pull_telemetry"
    auth_config = get_auth_config(integration)
    device_refs = action_config.device_refs or None
    device_uids = action_config.device_uids or None

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

    if use_realtime:
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title="Fetching telemetry from Kineis realtime API",
            level=LogLevel.INFO,
            data={"checkpoint": checkpoint},
        )
        messages, new_checkpoint = await fetch_telemetry_realtime(
            integration_id=integration_id,
            username=auth_config.username,
            password=auth_config.password.get_secret_value(),
            checkpoint=checkpoint,
            device_refs=device_refs,
            device_uids=device_uids,
            retrieve_metadata=action_config.retrieve_metadata,
            retrieve_raw_data=action_config.retrieve_raw_data,
            client_id=auth_config.client_id,
        )
    else:
        to_time = _utc_now()
        from_time = to_time - timedelta(hours=action_config.lookback_hours)
        from_str = _format_utc(from_time)
        to_str = _format_utc(to_time)
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title="Fetching telemetry from Kineis bulk API",
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

    if not messages and not (use_realtime and doppler_buffer):
        log_data = {"messages_fetched": 0}
        if use_realtime:
            log_data["checkpoint_from"] = checkpoint
            # new_checkpoint is always assigned here: use_realtime is True only after fetch_telemetry_realtime ran
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
        # Persist checkpoint+buffer before sending: like the original checkpoint advance,
        # a send failure after this point is intentionally backstopped by the daily backfill.
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

    sent_total = 0
    for batch in generate_batches(observations, OBSERVATION_BATCH_SIZE):
        await send_observations_to_gundi(
            observations=list(batch),
            integration_id=integration.id,
        )
        sent_total += len(batch)

    # Build a single meaningful summary log
    summary_data: Dict = {
        "messages_fetched": len(messages),
        "observations_sent": sent_total,
    }
    if transform_result.msg_types_seen:
        summary_data["message_types"] = transform_result.msg_types_seen
    if skipped:
        summary_data["skipped"] = skipped
        summary_data["skip_reasons"] = transform_result.skip_reasons
    summary_data.update(doppler_summary)
    if transform_result.devices_with_location:
        summary_data["devices_with_location"] = sorted(transform_result.devices_with_location)
    if transform_result.devices_without_location:
        summary_data["devices_without_location"] = sorted(transform_result.devices_without_location)

    if sent_total > 0:
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title=f"Sent {sent_total} observations to Gundi ({skipped} messages skipped)" if skipped else f"Sent {sent_total} observations to Gundi",
            level=LogLevel.INFO,
            data=summary_data,
        )
    elif doppler_summary.get("doppler_held_unsettled") or doppler_summary.get("doppler_buffered"):
        held = doppler_summary.get("doppler_held_unsettled") or doppler_summary.get("doppler_buffered")
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title=f"No observations sent; {held} Doppler fix(es) awaiting settle window",
            level=LogLevel.INFO,
            data=summary_data,
        )
    else:
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title=f"No observations could be created from {len(messages)} messages",
            level=LogLevel.WARNING,
            data=summary_data,
        )
    logger.info(
        "pull_telemetry summary: messages=%d sent=%d skipped=%d devices_with_location=%s devices_without_location=%s",
        len(messages), sent_total, skipped,
        sorted(transform_result.devices_with_location),
        sorted(transform_result.devices_without_location),
    )

    return {
        "messages_fetched": len(messages),
        "observations_sent": sent_total,
        "skipped": skipped,
    }


@crontab_schedule("0 2 * * *")  # Daily at 02:00 UTC
@activity_logger()
async def action_backfill_telemetry(integration, action_config: BackfillTelemetryConfiguration):
    """
    Daily bulk backfill: re-fetch last N hours via bulk API to catch messages
    whose Doppler locations were computed after the realtime checkpoint advanced.
    EarthRanger deduplication handles overlap with realtime data.
    Credentials come from the Auth action.
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
    observations, dedup_stats = collapse_doppler_revisions(
        transform_result.observations,
        settle_window=timedelta(hours=action_config.doppler_settle_hours),
        now=_utc_now(),
    )
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
    if dedup_stats["revisions_collapsed"]:
        summary_data["doppler_revisions_collapsed"] = dedup_stats["revisions_collapsed"]
    if dedup_stats["held_unsettled"]:
        summary_data["doppler_held_unsettled"] = dedup_stats["held_unsettled"]
    if transform_result.devices_with_location:
        summary_data["devices_with_location"] = sorted(transform_result.devices_with_location)
    if transform_result.devices_without_location:
        summary_data["devices_without_location"] = sorted(transform_result.devices_without_location)

    if sent_total > 0:
        title = (
            f"Backfill: sent {sent_total} observations to Gundi ({skipped} skipped)"
            if skipped else f"Backfill: sent {sent_total} observations to Gundi"
        )
        level = LogLevel.INFO
    elif dedup_stats["held_unsettled"]:
        title = f"Backfill: no observations sent; {dedup_stats['held_unsettled']} Doppler fix(es) held pending settle window"
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
    logger.info(
        "backfill_telemetry summary: messages=%d sent=%d skipped=%d devices_with_location=%s devices_without_location=%s",
        len(messages), sent_total, skipped,
        sorted(transform_result.devices_with_location),
        sorted(transform_result.devices_without_location),
    )

    return {
        "messages_fetched": len(messages),
        "observations_sent": sent_total,
        "skipped": skipped,
    }
