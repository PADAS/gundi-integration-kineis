"""
Map Kineis/CLS telemetry messages to Gundi observation schema (CONNECTORS-836).

Uses docs/kineis-api-reference.md and docs/kineis-api-samples/ for field names.
Goal: read GPS fixes (gpsLocLat/Lon, gpsLocDatetime), transform, and send as observations.
"""

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from app.datasource.kineis import classify_doppler_confidence, is_zero_zero

logger = logging.getLogger(__name__)

GUNDI_OBSERVATION_TYPE = "tracking-device"


class LocationType(str, Enum):
    GPS = "gps"
    DOPPLER = "doppler"
    NONE = "none"


def _is_valid_coordinate(lat: float, lon: float) -> bool:
    """Check if coordinates are within valid ranges and not zero-zero."""
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return False
    if is_zero_zero(lat_f, lon_f):
        return False
    if not (-90.0 <= lat_f <= 90.0) or not (-180.0 <= lon_f <= 180.0):
        return False
    return True


def classify_message_location(message: Dict[str, Any]) -> Tuple[LocationType, Any, Any]:
    """
    Classify a telemetry message's location source.

    Priority:
    1. GPS flat fields (gpsLocLat/gpsLocLon)
    2. Legacy nested dicts (gps, location, position keys with lat/lon)
    3. Doppler flat fields (dopplerLocLat/dopplerLocLon)
    4. No usable location

    Coordinates at (0, 0) or outside valid ranges are rejected.
    Returns (location_type, lat, lon). Coordinates are raw values.
    """
    # 1. GPS flat fields
    gps_lat = message.get("gpsLocLat")
    gps_lon = message.get("gpsLocLon")
    if gps_lat is not None and gps_lon is not None and _is_valid_coordinate(gps_lat, gps_lon):
        return LocationType.GPS, gps_lat, gps_lon

    # 2. Legacy nested dicts
    gps = message.get("gps") or message.get("location") or message.get("position") or {}
    if isinstance(gps, dict):
        lat = gps.get("lat") if gps.get("lat") is not None else gps.get("latitude")
        lon = gps.get("lon") if gps.get("lon") is not None else (gps.get("longitude") or gps.get("lng"))
        if lat is not None and lon is not None and _is_valid_coordinate(lat, lon):
            return LocationType.GPS, lat, lon

    # 3. Doppler flat fields
    doppler_lat = message.get("dopplerLocLat")
    doppler_lon = message.get("dopplerLocLon")
    if doppler_lat is not None and doppler_lon is not None and _is_valid_coordinate(doppler_lat, doppler_lon):
        return LocationType.DOPPLER, doppler_lat, doppler_lon

    # 4. No usable location
    return LocationType.NONE, None, None


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
    Map a single Kineis telemetry message to a Gundi observation.

    Classifies location source (GPS, Doppler, or none) before extracting coordinates.
    Returns None if location or required fields are missing.
    """
    # Source: use deviceRef only (per Gundi requirement)
    device_ref = message.get("deviceRef")
    if device_ref is None:
        logger.debug("Telemetry message missing deviceRef, skipping")
        return None
    source = str(device_ref)

    # Timestamp: prefer GPS fix time, then Doppler fix time, then message timestamps
    recorded_at = (
        message.get("gpsLocDatetime")
        or message.get("dopplerDatetime")
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

    # Location: classify then extract
    location_type, lat, lon = classify_message_location(message)
    if location_type == LocationType.NONE:
        logger.debug("Telemetry message missing lat/lon, skipping: %s", source)
        return None
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        logger.debug("Invalid lat/lon for %s: %s, %s", source, lat, lon)
        return None

    # Additional: include all record properties with original names
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

    # Location quality metadata
    if location_type == LocationType.GPS:
        location_confidence = "high"
        location_error_m = None
    elif location_type == LocationType.DOPPLER:
        location_confidence = classify_doppler_confidence(
            doppler_class=message.get("dopplerLocClass"),
            error_m=message.get("dopplerLocErrorRadius"),
            nb_msg=message.get("dopplerNbMsg"),
        )
        location_error_m = message.get("dopplerLocErrorRadius")
    else:
        location_confidence = None
        location_error_m = None

    additional.update({
        "location_type": location_type.value,
        "location_confidence": location_confidence,
        "location_error_m": location_error_m,
    })

    return {
        "source": source,
        "source_name": source_name,
        "type": GUNDI_OBSERVATION_TYPE,
        "subject_type": "unassigned",
        "recorded_at": recorded_at,
        "location": {"lat": lat_f, "lon": lon_f},
        "location_type": location_type.value,
        "location_confidence": location_confidence,
        "location_error_m": location_error_m,
        "additional": additional or {},
    }


def telemetry_batch_to_observations(
    messages: List[Dict[str, Any]],
    device_uid_to_customer_name: Optional[Dict[int, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Map a list of telemetry messages to Gundi observations. Skips invalid messages.
    """
    observations = []
    for msg in messages:
        obs = telemetry_to_observation(
            msg,
            device_uid_to_customer_name=device_uid_to_customer_name,
        )
        if obs:
            observations.append(obs)
    return observations


def _has_no_location(message: Dict[str, Any]) -> bool:
    """Check if a message has no usable location."""
    return classify_message_location(message)[0] == LocationType.NONE


def _has_no_timestamp(message: Dict[str, Any]) -> bool:
    """Check if a message has no usable timestamp."""
    ts = (
        message.get("gpsLocDatetime")
        or message.get("dopplerDatetime")
        or message.get("recordedAt")
        or message.get("msgDatetime")
        or message.get("acqDatetime")
        or message.get("timestamp")
        or message.get("receivedAt")
        or message.get("date")
        or message.get("msgTs")
        or message.get("acqTs")
        or message.get("gpsLocTs")
    )
    return ts is None


def _has_any_coordinates(message: Dict[str, Any]) -> bool:
    """Return True if the message contains any coordinate fields (regardless of validity)."""
    if message.get("gpsLocLat") is not None and message.get("gpsLocLon") is not None:
        return True
    if message.get("dopplerLocLat") is not None and message.get("dopplerLocLon") is not None:
        return True
    gps_dict = message.get("gps") or message.get("location") or message.get("position") or {}
    if isinstance(gps_dict, dict):
        lat = gps_dict.get("lat") if gps_dict.get("lat") is not None else gps_dict.get("latitude")
        lon = gps_dict.get("lon") if gps_dict.get("lon") is not None else (gps_dict.get("longitude") or gps_dict.get("lng"))
        if lat is not None and lon is not None:
            return True
    return False


def _has_coordinates_but_zero_zero(message: Dict[str, Any]) -> bool:
    """Return True only when all present coordinate pairs are exactly (0, 0)."""
    gps_lat = message.get("gpsLocLat")
    gps_lon = message.get("gpsLocLon")
    has_gps = gps_lat is not None and gps_lon is not None

    nested_lat, nested_lon = None, None
    gps_dict = message.get("gps") or message.get("location") or message.get("position") or {}
    if isinstance(gps_dict, dict):
        nested_lat = gps_dict.get("lat") if gps_dict.get("lat") is not None else gps_dict.get("latitude")
        nested_lon = gps_dict.get("lon") if gps_dict.get("lon") is not None else (gps_dict.get("longitude") or gps_dict.get("lng"))
    has_nested = nested_lat is not None and nested_lon is not None

    doppler_lat = message.get("dopplerLocLat")
    doppler_lon = message.get("dopplerLocLon")
    has_doppler = doppler_lat is not None and doppler_lon is not None

    coord_pairs = []
    if has_gps:
        coord_pairs.append((gps_lat, gps_lon))
    if has_nested:
        coord_pairs.append((nested_lat, nested_lon))
    if has_doppler:
        coord_pairs.append((doppler_lat, doppler_lon))

    if not coord_pairs:
        return False

    return all(is_zero_zero(lat, lon) for lat, lon in coord_pairs)


class TransformResult:
    """Result of a detailed batch transform with skip reason tracking."""

    def __init__(self):
        self.observations: List[Dict[str, Any]] = []
        self.total_skipped: int = 0
        self.skipped_no_device_ref: int = 0
        self.skipped_no_location: int = 0
        self.skipped_no_timestamp: int = 0
        self.skip_reasons: Dict[str, int] = {}
        self.msg_types_seen: Dict[str, int] = {}
        self.location_types_seen: Dict[str, int] = {}


def telemetry_batch_to_observations_detailed(
    messages: List[Dict[str, Any]],
    device_uid_to_customer_name: Optional[Dict[int, str]] = None,
) -> TransformResult:
    """
    Map a list of telemetry messages to observations with detailed skip tracking.
    """
    result = TransformResult()

    for msg in messages:
        # Track message types
        msg_type = msg.get("msgType")
        if msg_type:
            result.msg_types_seen[msg_type] = result.msg_types_seen.get(msg_type, 0) + 1

        # Classify skip reason before attempting transform
        if msg.get("deviceRef") is None:
            result.total_skipped += 1
            result.skipped_no_device_ref += 1
            result.skip_reasons["no_device_ref"] = result.skip_reasons.get("no_device_ref", 0) + 1
            continue

        if _has_no_timestamp(msg):
            result.total_skipped += 1
            result.skipped_no_timestamp += 1
            result.skip_reasons["no_timestamp"] = result.skip_reasons.get("no_timestamp", 0) + 1
            continue

        if _has_no_location(msg):
            result.total_skipped += 1
            result.skipped_no_location += 1
            if _has_coordinates_but_zero_zero(msg):
                reason = "zero_zero_coordinates"
            elif _has_any_coordinates(msg):
                reason = "invalid_coordinates"
            else:
                reason = "no_location"
            result.skip_reasons[reason] = result.skip_reasons.get(reason, 0) + 1
            continue

        obs = telemetry_to_observation(msg, device_uid_to_customer_name=device_uid_to_customer_name)
        if obs:
            result.observations.append(obs)
            loc_type = obs.get("location_type", "unknown")
            result.location_types_seen[loc_type] = result.location_types_seen.get(loc_type, 0) + 1
        else:
            # Unexpected skip (e.g. invalid float conversion)
            result.total_skipped += 1
            result.skip_reasons["other"] = result.skip_reasons.get("other", 0) + 1

    return result
