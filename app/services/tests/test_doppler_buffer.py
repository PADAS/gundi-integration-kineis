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


def test_lower_revision_does_not_replace_buffered_fix():
    """A later, lower-revision re-send must not overwrite the higher buffered revision."""
    recent2 = (NOW - timedelta(minutes=58)).isoformat().replace("+00:00", "")
    recent0 = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "")
    rev2 = _obs("45020", 11, 2, recent2, -46.68, 168.31, cls="2")
    rev0 = _obs("45020", 11, 0, recent0, -46.60, 168.33, cls="B")
    buffer = {_key("45020", 11): rev2}
    emit, new_buffer, stats = reconcile_doppler_buffer(buffer, [rev0], SETTLE, NOW)
    assert emit == []
    assert new_buffer[_key("45020", 11)]["additional"]["dopplerRevision"] == 2
    assert stats["revisions_collapsed"] == 1
