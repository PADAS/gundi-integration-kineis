"""Tests for Kineis telemetry to Gundi observation mapping (CONNECTORS-836)."""

import pytest

from app.actions.transformers import (
    LocationType,
    classify_message_location,
    telemetry_to_observation,
    telemetry_batch_to_observations,
    telemetry_batch_to_observations_detailed,
    GUNDI_OBSERVATION_TYPE,
)


def test_telemetry_to_observation_valid():
    """Map a valid telemetry message with deviceRef, gps, recordedAt."""
    msg = {
        "deviceRef": "238883",
        "recordedAt": "2024-01-15T10:30:00.000Z",
        "gps": {"lat": -1.5, "lon": 30.2},
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "238883"
    assert obs["type"] == GUNDI_OBSERVATION_TYPE
    assert obs["recorded_at"] == "2024-01-15T10:30:00.000Z"
    assert obs["location"] == {"lat": -1.5, "lon": 30.2}
    assert "additional" in obs


def test_telemetry_to_observation_requires_device_ref():
    """Skip message when deviceRef is missing (source must be deviceRef for Gundi)."""
    msg = {
        "deviceUid": 62533,
        "timestamp": "2024-01-15T12:00:00Z",
        "lat": -2.0,
        "lon": 31.0,
    }
    obs = telemetry_to_observation(msg)
    assert obs is None


def test_telemetry_to_observation_missing_source_returns_none():
    """Return None when deviceRef is missing."""
    msg = {
        "recordedAt": "2024-01-15T10:00:00Z",
        "gps": {"lat": 1.0, "lon": 1.0},
    }
    assert telemetry_to_observation(msg) is None


def test_telemetry_to_observation_missing_location_returns_none():
    """Return None when lat/lon are missing."""
    msg = {
        "deviceRef": "D1",
        "recordedAt": "2024-01-15T10:00:00Z",
    }
    assert telemetry_to_observation(msg) is None


def test_telemetry_to_observation_missing_timestamp_returns_none():
    """Return None when no timestamp field."""
    msg = {
        "deviceRef": "D1",
        "gps": {"lat": 1.0, "lon": 1.0},
    }
    assert telemetry_to_observation(msg) is None


def test_telemetry_to_observation_additional_fields():
    """Additional contains full record with original property names plus location metadata."""
    msg = {
        "deviceRef": "D1",
        "recordedAt": "2024-01-15T10:00:00.000Z",
        "gps": {"lat": 5.0, "lon": 10.0, "speed": 5.2, "course": 90},
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    # All original fields are preserved in additional
    assert obs["additional"]["deviceRef"] == msg["deviceRef"]
    assert obs["additional"]["recordedAt"] == msg["recordedAt"]
    assert obs["additional"]["gps"] == msg["gps"]
    # Transformer enriches additional with location metadata
    assert obs["additional"]["location_type"] == "gps"
    assert obs["additional"]["location_confidence"] == "high"
    assert obs["additional"]["location_error_m"] is None
    assert obs["additional"]["gps"]["speed"] == 5.2
    assert obs["additional"]["gps"]["course"] == 90


def test_telemetry_batch_to_observations_skips_invalid():
    """Invalid messages are skipped; valid ones are returned. Source is deviceRef only."""
    messages = [
        {"deviceRef": "A", "recordedAt": "2024-01-15T10:00:00Z", "gps": {"lat": 5.0, "lon": 10.0}},
        {"deviceRef": "B"},  # missing location and timestamp
        {"deviceUid": 1, "timestamp": "2024-01-15T11:00:00Z", "lat": 1, "lon": 1},  # no deviceRef -> skipped
    ]
    result = telemetry_batch_to_observations(messages)
    assert len(result) == 1
    assert result[0]["source"] == "A"


def test_telemetry_to_observation_api_shape_msg_ts_gps_loc():
    """Map API-shaped message with msgTs (epoch ms) and gpsLocLat/gpsLocLon (bulk/realtime)."""
    msg = {
        "deviceRef": "238883",
        "msgTs": 1705312800000,  # 2024-01-15 10:00:00 UTC
        "gpsLocLat": -1.5,
        "gpsLocLon": 30.2,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "238883"
    assert obs["location"] == {"lat": -1.5, "lon": 30.2}
    assert "2024-01-15" in obs["recorded_at"] and ("Z" in obs["recorded_at"] or "+00:00" in obs["recorded_at"])


def test_telemetry_to_observation_api_shape_doppler_loc():
    """Map API-shaped message with dopplerLocLat/dopplerLocLon when GPS missing."""
    msg = {
        "deviceRef": "1788",
        "deviceUid": 1788,
        "acqTs": 1705316400000,
        "dopplerLocLat": -42.47,
        "dopplerLocLon": 173.63,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "1788"
    assert obs["location"] == {"lat": -42.47, "lon": 173.63}


def test_telemetry_to_observation_sample_response_gps_fix():
    """Map sample response shape (docs/kineis-api-samples): GPS fix time and extra fields."""
    # Matches retrieve-bulk-response / retrieve-realtime-response sample message
    msg = {
        "deviceMsgUid": 59220647342112780,
        "deviceUid": 67899,
        "deviceRef": "7896",
        "modemRef": "7896",
        "msgType": "operation-mo-pdrgroup",
        "msgDatetime": "2024-10-01T15:56:19.001Z",
        "acqDatetime": "2024-10-01T15:56:25.001Z",
        "gpsLocDatetime": "2024-10-01T15:56:18.001Z",
        "gpsLocLat": 20.45123,
        "gpsLocLon": 58.77856,
        "gpsLocAlt": 0,
        "gpsLocSpeed": 2.78,
        "gpsLocHeading": 67.45,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["source"] == "7896"
    assert obs["source_name"] == "7896"  # API has no device display name; we use deviceRef
    assert obs["location"] == {"lat": 20.45123, "lon": 58.77856}
    # Prefer GPS fix time for recorded_at
    assert obs["recorded_at"] == "2024-10-01T15:56:18.001Z"
    # Additional includes full record with original API names
    assert obs["additional"].get("gpsLocSpeed") == 2.78
    assert obs["additional"].get("gpsLocHeading") == 67.45
    assert obs["additional"].get("gpsLocAlt") == 0
    assert obs["additional"].get("msgType") == "operation-mo-pdrgroup"


def test_telemetry_to_observation_source_name_with_customer_name():
    """When device_uid_to_customer_name is provided, source_name is 'source (customerName)'."""
    msg = {
        "deviceUid": 67899,
        "deviceRef": "7896",
        "gpsLocDatetime": "2024-10-01T15:56:18.001Z",
        "gpsLocLat": 20.45123,
        "gpsLocLon": 58.77856,
    }
    device_uid_to_customer_name = {67899: "WILDLIFE COMPUTER"}
    obs = telemetry_to_observation(msg, device_uid_to_customer_name=device_uid_to_customer_name)
    assert obs is not None
    assert obs["source"] == "7896"
    assert obs["source_name"] == "7896 (WILDLIFE COMPUTER)"


def test_telemetry_to_observation_source_name_fallback_without_customer_name():
    """When deviceUid is not in device_uid_to_customer_name, source_name equals source."""
    msg = {
        "deviceUid": 99999,
        "deviceRef": "ref99",
        "gpsLocLat": 5.0,
        "gpsLocLon": 10.0,
        "msgTs": 1705312800000,
    }
    device_uid_to_customer_name = {67899: "WILDLIFE COMPUTER"}  # ref99 not in map
    obs = telemetry_to_observation(msg, device_uid_to_customer_name=device_uid_to_customer_name)
    assert obs is not None
    assert obs["source"] == "ref99"
    assert obs["source_name"] == "ref99"


def test_telemetry_batch_to_observations_source_name_with_customer_name():
    """When device list is passed, observations get source_name 'source (customerName)'."""
    messages = [
        {
            "deviceUid": 67899,
            "deviceRef": "7896",
            "gpsLocLat": 20.45,
            "gpsLocLon": 58.77,
            "msgTs": 1705312800000,
        },
    ]
    device_uid_to_customer_name = {67899: "WILDLIFE COMPUTER"}
    result = telemetry_batch_to_observations(
        messages,
        device_uid_to_customer_name=device_uid_to_customer_name,
    )
    assert len(result) == 1
    assert result[0]["source"] == "7896"
    assert result[0]["source_name"] == "7896 (WILDLIFE COMPUTER)"


def test_detailed_transform_tracks_skip_reasons():
    """Detailed transform returns skip reason counts and message type breakdown."""
    messages = [
        # Valid: has deviceRef, location, timestamp
        {"deviceRef": "A", "msgDatetime": "2024-01-15T10:00:00Z", "gpsLocLat": 5.0, "gpsLocLon": 10.0, "msgType": "operation-mo-pdrgroup"},
        # Skipped: no deviceRef
        {"deviceUid": 1, "msgDatetime": "2024-01-15T11:00:00Z", "gpsLocLat": 1, "gpsLocLon": 1, "msgType": "operation-mo-event"},
        # Skipped: no location
        {"deviceRef": "B", "msgDatetime": "2024-01-15T12:00:00Z", "msgType": "operation-mo-event"},
        # Skipped: no timestamp
        {"deviceRef": "C", "gpsLocLat": 2, "gpsLocLon": 2, "msgType": "operation-mo-pdrgroup"},
    ]
    result = telemetry_batch_to_observations_detailed(messages)
    assert len(result.observations) == 1
    assert result.observations[0]["source"] == "A"
    assert result.total_skipped == 3
    assert result.skipped_no_device_ref == 1
    assert result.skipped_no_location == 1
    assert result.skipped_no_timestamp == 1
    assert result.skip_reasons == {
        "no_device_ref": 1,
        "no_location": 1,
        "no_timestamp": 1,
    }
    assert result.msg_types_seen == {
        "operation-mo-pdrgroup": 2,
        "operation-mo-event": 2,
    }


def test_detailed_transform_all_valid():
    """When all messages are valid, skip counts are zero."""
    messages = [
        {"deviceRef": "A", "msgDatetime": "2024-01-15T10:00:00Z", "gpsLocLat": 5.0, "gpsLocLon": 10.0},
    ]
    result = telemetry_batch_to_observations_detailed(messages)
    assert len(result.observations) == 1
    assert result.total_skipped == 0
    assert result.skip_reasons == {}


def test_detailed_transform_empty_input():
    """Empty input returns empty result."""
    result = telemetry_batch_to_observations_detailed([])
    assert len(result.observations) == 0
    assert result.total_skipped == 0
    assert result.msg_types_seen == {}


# --- Classification tests ---


def test_classify_gps_flat_fields():
    """gpsLocLat/gpsLocLon → LocationType.GPS with correct coords."""
    msg = {"gpsLocLat": 20.45, "gpsLocLon": 58.77}
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.GPS
    assert lat == 20.45
    assert lon == 58.77


def test_classify_doppler_flat_fields():
    """dopplerLocLat/dopplerLocLon (no GPS) → LocationType.DOPPLER."""
    msg = {"dopplerLocLat": -42.47, "dopplerLocLon": 173.63}
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.DOPPLER
    assert lat == -42.47
    assert lon == 173.63


def test_classify_nested_gps_dict():
    """{"gps": {"lat": ..., "lon": ...}} → LocationType.GPS."""
    msg = {"gps": {"lat": -1.5, "lon": 30.2}}
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.GPS
    assert lat == -1.5
    assert lon == 30.2


def test_classify_both_prefers_gps():
    """Message with both GPS and Doppler fields → LocationType.GPS."""
    msg = {
        "gpsLocLat": 20.45,
        "gpsLocLon": 58.77,
        "dopplerLocLat": -42.47,
        "dopplerLocLon": 173.63,
    }
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.GPS
    assert lat == 20.45
    assert lon == 58.77


def test_classify_no_location():
    """No GPS or Doppler fields → LocationType.NONE."""
    msg = {"deviceRef": "123", "msgDatetime": "2024-01-15T10:00:00Z"}
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.NONE
    assert lat is None
    assert lon is None


def test_classify_message_example(message_example):
    """docs/message-example.json shape (Doppler only, operation-mo-event) → LocationType.DOPPLER."""
    loc_type, lat, lon = classify_message_location(message_example)
    assert loc_type == LocationType.DOPPLER
    assert lat == -42.47563
    assert lon == 173.63476


def test_observation_location_type_gps():
    """GPS observation has location_type 'gps'."""
    msg = {
        "deviceRef": "A",
        "msgDatetime": "2024-01-15T10:00:00Z",
        "gpsLocLat": 20.45,
        "gpsLocLon": 58.77,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["location_type"] == "gps"


def test_observation_location_type_doppler():
    """Doppler observation has location_type 'doppler'."""
    msg = {
        "deviceRef": "A",
        "msgDatetime": "2024-01-15T10:00:00Z",
        "dopplerLocLat": -42.47,
        "dopplerLocLon": 173.63,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["location_type"] == "doppler"


def test_doppler_uses_doppler_datetime():
    """Doppler-only message with dopplerDatetime uses it for recorded_at."""
    msg = {
        "deviceRef": "45009",
        "dopplerDatetime": "2026-03-10T19:48:46.381Z",
        "msgDatetime": "2026-03-10T19:48:46.318Z",
        "dopplerLocLat": -42.47563,
        "dopplerLocLon": 173.63476,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    # dopplerDatetime comes after gpsLocDatetime in priority, but before msgDatetime
    # Since no gpsLocDatetime, dopplerDatetime is used
    assert obs["recorded_at"] == "2026-03-10T19:48:46.381Z"
    assert obs["location_type"] == "doppler"


# --- Zero-zero coordinate filtering tests ---


def test_classify_rejects_zero_zero_doppler():
    """Doppler at (0, 0) → LocationType.NONE."""
    msg = {"dopplerLocLat": 0.0, "dopplerLocLon": 0.0}
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.NONE
    assert lat is None
    assert lon is None


def test_classify_rejects_zero_zero_gps():
    """GPS at (0, 0) → LocationType.NONE."""
    msg = {"gpsLocLat": 0.0, "gpsLocLon": 0.0}
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.NONE
    assert lat is None
    assert lon is None


def test_classify_gps_zero_zero_falls_through_to_doppler():
    """GPS (0, 0) + valid Doppler → LocationType.DOPPLER."""
    msg = {
        "gpsLocLat": 0.0,
        "gpsLocLon": 0.0,
        "dopplerLocLat": -42.47,
        "dopplerLocLon": 173.63,
    }
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.DOPPLER
    assert lat == -42.47
    assert lon == 173.63


def test_classify_rejects_out_of_range():
    """Out-of-range coordinates → LocationType.NONE."""
    msg = {"gpsLocLat": 999.0, "gpsLocLon": 58.77}
    loc_type, lat, lon = classify_message_location(msg)
    assert loc_type == LocationType.NONE

    msg2 = {"gpsLocLat": 20.0, "gpsLocLon": -999.0}
    loc_type2, _, _ = classify_message_location(msg2)
    assert loc_type2 == LocationType.NONE


# --- Doppler confidence and error radius tests ---


def test_observation_doppler_confidence():
    """Doppler observation has location_confidence from classifier."""
    msg = {
        "deviceRef": "A",
        "msgDatetime": "2024-01-15T10:00:00Z",
        "dopplerLocLat": -42.47,
        "dopplerLocLon": 173.63,
        "dopplerLocClass": "3",
        "dopplerLocErrorRadius": 200.0,
        "dopplerNbMsg": 4,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["location_confidence"] == "high"


def test_observation_doppler_error_radius():
    """Doppler observation has location_error_m from message."""
    msg = {
        "deviceRef": "A",
        "msgDatetime": "2024-01-15T10:00:00Z",
        "dopplerLocLat": -42.47,
        "dopplerLocLon": 173.63,
        "dopplerLocErrorRadius": 1547.0,
        "dopplerLocClass": "B",
        "dopplerNbMsg": 2,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["location_error_m"] == 1547.0
    assert obs["location_confidence"] == "low"


def test_observation_gps_confidence_high():
    """GPS observation has location_confidence 'high' and no error radius."""
    msg = {
        "deviceRef": "A",
        "msgDatetime": "2024-01-15T10:00:00Z",
        "gpsLocLat": 20.45,
        "gpsLocLon": 58.77,
    }
    obs = telemetry_to_observation(msg)
    assert obs is not None
    assert obs["location_confidence"] == "high"
    assert obs["location_error_m"] is None


def test_detailed_transform_tracks_zero_zero():
    """Zero-zero messages counted in skip reasons as 'zero_zero_coordinates'."""
    messages = [
        # Valid
        {"deviceRef": "A", "msgDatetime": "2024-01-15T10:00:00Z", "gpsLocLat": 5.0, "gpsLocLon": 10.0},
        # Zero-zero GPS
        {"deviceRef": "B", "msgDatetime": "2024-01-15T11:00:00Z", "gpsLocLat": 0.0, "gpsLocLon": 0.0},
        # Zero-zero Doppler
        {"deviceRef": "C", "msgDatetime": "2024-01-15T12:00:00Z", "dopplerLocLat": 0.0, "dopplerLocLon": 0.0},
    ]
    result = telemetry_batch_to_observations_detailed(messages)
    assert len(result.observations) == 1
    assert result.total_skipped == 2
    assert result.skipped_no_location == 2
    assert result.skip_reasons.get("zero_zero_coordinates") == 2
    assert "no_location" not in result.skip_reasons
