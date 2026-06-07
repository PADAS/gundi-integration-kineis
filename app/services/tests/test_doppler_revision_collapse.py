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


def test_same_locid_different_devices_not_collapsed():
    """Two devices sharing a dopplerLocId must not be collapsed into one."""
    a = _obs("45020", 11, 2, "2026-05-17T01:00:00", -46.6, 168.3)
    b = _obs("45021", 11, 0, "2026-05-17T01:00:00", -46.7, 168.4)
    kept, stats = collapse_doppler_revisions([a, b], SETTLE, NOW)
    assert len(kept) == 2
    assert stats["revisions_collapsed"] == 0
