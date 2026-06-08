from typing import List, Optional

import pydantic
from pydantic import root_validator

from app.actions.core import AuthActionConfiguration, ExecutableActionMixin, PullActionConfiguration
from app.services.errors import ConfigurationNotFound
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions, find_config_for_action

# action_backfill_telemetry runs daily (see @crontab_schedule("0 2 * * *") in handlers.py).
# A Doppler fix held by the settle window must still fall inside the NEXT daily
# backfill's lookback window to ever be emitted, so lookback must exceed this interval
# by at least the settle window.
BACKFILL_INTERVAL_HOURS = 24


class AuthenticateKineisConfig(AuthActionConfiguration, ExecutableActionMixin):
    """Configuration for the Kineis auth action (portal credential verification)."""

    username: str = pydantic.Field(
        ...,
        title="Username",
        description="CLS/Kineis API username"
    )
    password: pydantic.SecretStr = pydantic.Field(
        ...,
        title="Password",
        description="CLS/Kineis API password",
        format="password"
    )
    client_id: str = pydantic.Field(
        "api-telemetry",
        title="Client ID",
        description="OAuth client_id for token endpoint",
    )

    ui_global_options = GlobalUISchemaOptions(
        order=["username", "password", "client_id"],
    )


def get_auth_config(integration):
    """Get Kineis auth credentials from the integration's auth action config."""
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth",
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            "are missing. Please configure the Auth action in the portal."
        )
    return AuthenticateKineisConfig.parse_obj(auth_config.data)


class PullTelemetryConfiguration(PullActionConfiguration):
    """Configuration for the Kineis pull_telemetry action (CONNECTORS-836). Credentials come from the Auth action."""

    lookback_hours: int = FieldWithUIOptions(
        4,
        ge=1,
        le=2160,
        title="Lookback hours",
        description="Hours to look back for telemetry (UTC time window)",
        ui_options=UIOptions(
            widget="range",  # slider
        )
    )
    page_size: int = FieldWithUIOptions(
        100,
        ge=1,
        le=500,
        title="Page size",
        description="Bulk API pagination page size",
        ui_options=UIOptions(
            widget="range",  # slider
        )

    )
    device_refs: Optional[List[str]] = FieldWithUIOptions(
        default=None,
        title="Device refs",
        description="Optional list of device refs (string IDs) to filter",
        # ui_options=UIOptions(widget="textarea"),
    )
    device_uids: Optional[List[int]] = FieldWithUIOptions(
        default=None,
        title="Device UIDs",
        description="Optional list of device UIDs (numeric) to filter",
        # ui_options=UIOptions(widget="textarea"),
    )
    retrieve_metadata: bool = pydantic.Field(
        True,
        title="Retrieve metadata",
        description="Include metadata in bulk response",
    )
    retrieve_raw_data: bool = pydantic.Field(
        True,
        title="Retrieve raw data",
        description="Include raw data in bulk response",
    )
    use_realtime: bool = pydantic.Field(
        True,
        title="Use realtime API",
        description="When enabled, use the realtime checkpoint API for scheduled pulls (only new data since last run). When disabled or on first run, use bulk API with lookback window.",
    )
    doppler_settle_hours: int = FieldWithUIOptions(
        6,
        ge=0,
        le=48,
        title="Doppler settle hours",
        description="Hold Doppler locations until they are at least this many hours old, so CLS revisions finalize before sending. 0 disables holding (collapse still applies within a batch). Held fixes are intentionally suppressed until older than this window and emitted by a later run; on realtime pulls re-emission relies on the daily backfill action, so enable backfill_telemetry when this is > 0 or held fixes are never sent. The doppler_held_unsettled stat counts currently-held fixes and is expected, not a failure.",
        ui_options=UIOptions(
            widget="range",  # slider
        ),
    )

    @root_validator
    def device_filter_single(cls, values):
        """API allows only one of deviceRefs or deviceUids (manual 1.3.1.2)."""
        refs = values.get("device_refs") or []
        uids = values.get("device_uids") or []
        if refs and uids:
            raise ValueError(
                "Provide only one of device_refs or device_uids; the API does not accept both."
            )
        return values

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


class BackfillTelemetryConfiguration(PullActionConfiguration):
    """
    Daily bulk backfill config. Re-fetches the last N hours via the bulk API
    to pick up messages whose Doppler locations were computed late.
    Credentials come from the Auth action.
    """

    lookback_hours: int = FieldWithUIOptions(
        48,
        ge=1,
        le=168,
        title="Lookback hours",
        description="Hours to look back. Default 48h; max 168h (7 days). When doppler_settle_hours > 0 this must be >= 24h (the daily backfill interval) + doppler_settle_hours, so fixes held by the settle window are re-fetched on the next run instead of being dropped.",
        ui_options=UIOptions(widget="range"),
    )
    page_size: int = FieldWithUIOptions(
        100,
        ge=1,
        le=500,
        title="Page size",
        description="Bulk API pagination page size",
        ui_options=UIOptions(widget="range"),
    )
    doppler_settle_hours: int = FieldWithUIOptions(
        6,
        ge=0,
        le=48,
        title="Doppler settle hours",
        description="Hold Doppler locations until they are at least this many hours old, so CLS revisions finalize before sending. 0 disables holding (collapse still applies within a batch). Held fixes are intentionally suppressed until older than this window and emitted by a later run; on realtime pulls re-emission relies on the daily backfill action, so enable backfill_telemetry when this is > 0 or held fixes are never sent. The doppler_held_unsettled stat counts currently-held fixes and is expected, not a failure.",
        ui_options=UIOptions(widget="range"),
    )
    device_refs: Optional[List[str]] = FieldWithUIOptions(
        default=None,
        title="Device refs",
        description="Optional list of device refs (string IDs) to filter",
    )
    device_uids: Optional[List[int]] = FieldWithUIOptions(
        default=None,
        title="Device UIDs",
        description="Optional list of device UIDs (numeric) to filter",
    )
    retrieve_metadata: bool = pydantic.Field(
        True,
        title="Retrieve metadata",
        description="Include metadata in bulk response",
    )
    retrieve_raw_data: bool = pydantic.Field(
        True,
        title="Retrieve raw data",
        description="Include raw data in bulk response",
    )

    @root_validator
    def device_filter_single(cls, values):
        """API allows only one of deviceRefs or deviceUids (manual 1.3.1.2)."""
        refs = values.get("device_refs") or []
        uids = values.get("device_uids") or []
        if refs and uids:
            raise ValueError(
                "Provide only one of device_refs or device_uids; the API does not accept both."
            )
        return values

    @root_validator
    def lookback_covers_settle_window(cls, values):
        """When settling is enabled, the daily backfill must look back far enough to
        re-fetch fixes held on the previous run, or they are silently dropped."""
        settle = values.get("doppler_settle_hours") or 0
        lookback = values.get("lookback_hours") or 0
        if settle > 0:
            required = BACKFILL_INTERVAL_HOURS + settle
            if lookback < required:
                raise ValueError(
                    f"lookback_hours ({lookback}) must be >= {required} when doppler_settle_hours "
                    f"is {settle} (daily backfill interval {BACKFILL_INTERVAL_HOURS}h + settle window), "
                    "so Doppler fixes held by the settle window are re-fetched and emitted on the "
                    "next backfill run instead of being dropped."
                )
        return values

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
