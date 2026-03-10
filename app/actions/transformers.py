"""
Map Kineis/CLS telemetry messages to Gundi observation schema (CONNECTORS-836).

Uses docs/kineis-api-reference.md and docs/kineis-api-samples/ for field names.
Goal: read GPS fixes (gpsLocLat/Lon, gpsLocDatetime), transform, and send as observations.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

GUNDI_OBSERVATION_TYPE = "tracking-device"


def _normalize_recorded_at(value: Any) -> Optional[str]:
    """Convert timestamp to ISO string with Z (UTC)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0 if value > 1e12 else value, tz=timezone.utc).isoformat()
    s = str(value).strip()
    if not s:
        return None
    if not s.endswith("Z") and "+" not in s and (len(s) < 6 or s[-6] not in "+-"):
        s = s + "Z"
    return s


def telemetry_to_observation(
    message: Dict[str, Any],
    device_uid_to_customer_name: Optional[Dict[int, str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Map a single Kineis telemetry message to a Gundi observation (GPS fix).

    Prefers GPS location (gpsLocLat/Lon) and GPS fix time (gpsLocDatetime) when present;
    falls back to Doppler location and message timestamps. Returns None if location
    or required fields are missing. source is always deviceRef. source_name is
    "source (customerName)" when the device list provides a customerName for the
    message's deviceUid; otherwise source_name equals source.
    Messages without deviceRef are skipped.
    """
    # Source: use deviceRef only (per Gundi requirement)
    device_ref = message.get("deviceRef")
    if device_ref is None:
        logger.debug("Telemetry message missing deviceRef, skipping")
        return None
    source = str(device_ref)

    # Timestamp: for GPS fixes prefer gpsLocDatetime (fix time), then msgDatetime/acqDatetime, then msgTs/acqTs
    recorded_at = (
        message.get("gpsLocDatetime")  # GPS fix timestamp when using GPS location
        or message.get("recordedAt")
        or message.get("msgDatetime")
        or message.get("acqDatetime")
        or message.get("timestamp")
        or message.get("receivedAt")
        or message.get("date")
    )
    if recorded_at is None:
        msg_ts = message.get("msgTs") or message.get("acqTs") or message.get("gpsLocTs")
        if msg_ts is not None:
            recorded_at = datetime.fromtimestamp(msg_ts / 1000.0, tz=timezone.utc).isoformat()
    recorded_at = _normalize_recorded_at(recorded_at)
    if not recorded_at:
        logger.debug("Telemetry message missing timestamp, skipping: %s", source)
        return None

    # Location: prefer GPS (gpsLocLat/Lon), then Doppler (dopplerLocLat/Lon), then legacy shapes
    lat = None
    lon = None
    gps = message.get("gps") or message.get("location") or message.get("position") or {}
    if isinstance(gps, dict):
        lat = gps.get("lat") if gps.get("lat") is not None else gps.get("latitude")
        lon = gps.get("lon") if gps.get("lon") is not None else (gps.get("longitude") or gps.get("lng"))
    if lat is None:
        lat = message.get("gpsLocLat") if message.get("gpsLocLat") is not None else message.get("lat") or message.get("latitude")
    if lon is None:
        lon = message.get("gpsLocLon") if message.get("gpsLocLon") is not None else message.get("lon") or message.get("longitude")
    if lat is None:
        lat = message.get("dopplerLocLat")
    if lon is None:
        lon = message.get("dopplerLocLon")

    if lat is None or lon is None:
        logger.debug("Telemetry message missing lat/lon, skipping: %s", source)
        return None

    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        logger.debug("Invalid lat/lon for %s: %s, %s", source, lat, lon)
        return None

    # Additional: include all record properties with original names (feedback: full record in additional)
    additional: Dict[str, Any] = dict(message)

    # source_name: "source (customerName)" when device list provides customerName; else source
    device_uid = message.get("deviceUid")
    if (
        device_uid_to_customer_name
        and device_uid is not None
        and device_uid in device_uid_to_customer_name
        and device_uid_to_customer_name[device_uid]
    ):
        source_name = f"{source} ({device_uid_to_customer_name[device_uid]})"
    else:
        source_name = source

    return {
        "source": source,
        "source_name": source_name,
        "type": GUNDI_OBSERVATION_TYPE,
        "subject_type": "unassigned",
        "recorded_at": recorded_at,
        "location": {"lat": lat_f, "lon": lon_f},
        "additional": additional or {},
    }


class TransformResult:
    """Result of batch transformation with skip reason counts."""

    def __init__(self):
        self.observations: List[Dict[str, Any]] = []
        self.skipped_no_device_ref: int = 0
        self.skipped_no_timestamp: int = 0
        self.skipped_no_location: int = 0
        self.skipped_invalid_location: int = 0
        self.msg_types_seen: Dict[str, int] = {}

    @property
    def total_skipped(self) -> int:
        return (
            self.skipped_no_device_ref
            + self.skipped_no_timestamp
            + self.skipped_no_location
            + self.skipped_invalid_location
        )

    @property
    def skip_reasons(self) -> Dict[str, int]:
        reasons = {}
        if self.skipped_no_device_ref:
            reasons["no_device_ref"] = self.skipped_no_device_ref
        if self.skipped_no_timestamp:
            reasons["no_timestamp"] = self.skipped_no_timestamp
        if self.skipped_no_location:
            reasons["no_location"] = self.skipped_no_location
        if self.skipped_invalid_location:
            reasons["invalid_location"] = self.skipped_invalid_location
        return reasons


def telemetry_batch_to_observations(
    messages: List[Dict[str, Any]],
    device_uid_to_customer_name: Optional[Dict[int, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Map a list of telemetry messages to Gundi observations. Skips invalid messages.
    When device_uid_to_customer_name is provided, source_name is "source (customerName)" when available.
    """
    result = telemetry_batch_to_observations_detailed(
        messages, device_uid_to_customer_name=device_uid_to_customer_name,
    )
    return result.observations


def telemetry_batch_to_observations_detailed(
    messages: List[Dict[str, Any]],
    device_uid_to_customer_name: Optional[Dict[int, str]] = None,
) -> TransformResult:
    """
    Map a list of telemetry messages to Gundi observations with detailed skip tracking.
    Returns a TransformResult with observations, skip reason counts, and message type breakdown.
    """
    result = TransformResult()
    for msg in messages:
        msg_type = msg.get("msgType", "unknown")
        result.msg_types_seen[msg_type] = result.msg_types_seen.get(msg_type, 0) + 1

        obs = telemetry_to_observation(
            msg,
            device_uid_to_customer_name=device_uid_to_customer_name,
        )
        if obs:
            result.observations.append(obs)
        else:
            # Determine skip reason (same logic order as telemetry_to_observation)
            if msg.get("deviceRef") is None:
                result.skipped_no_device_ref += 1
            elif _has_no_timestamp(msg):
                result.skipped_no_timestamp += 1
            elif _has_no_location(msg):
                result.skipped_no_location += 1
            else:
                result.skipped_invalid_location += 1
    return result


def _has_no_timestamp(msg: Dict[str, Any]) -> bool:
    """Check if a message has no usable timestamp."""
    return not any(msg.get(f) for f in (
        "gpsLocDatetime", "recordedAt", "msgDatetime", "acqDatetime",
        "timestamp", "receivedAt", "date", "msgTs", "acqTs", "gpsLocTs",
    ))


def _has_no_location(msg: Dict[str, Any]) -> bool:
    """Check if a message has no usable location coordinates."""
    gps = msg.get("gps") or msg.get("location") or msg.get("position") or {}
    if isinstance(gps, dict):
        lat = gps.get("lat") if gps.get("lat") is not None else gps.get("latitude")
        lon = gps.get("lon") if gps.get("lon") is not None else (gps.get("longitude") or gps.get("lng"))
        if lat is not None and lon is not None:
            return False
    for lat_key in ("gpsLocLat", "lat", "latitude", "dopplerLocLat"):
        if msg.get(lat_key) is not None:
            for lon_key in ("gpsLocLon", "lon", "longitude", "dopplerLocLon"):
                if msg.get(lon_key) is not None:
                    return False
    return True
