import pytest


@pytest.fixture
def message_example():
    return {
        "deviceMsgUid": 6897720259090437530,
        "providerMsgId": 1481009527380779011,
        "msgType": "operation-mo-event",
        "deviceUid": 106499,
        "deviceRef": "45009",
        "modemRef": "45009",
        "msgDatetime": "2026-03-10T19:48:46.318",
        "acqDatetime": "2026-03-10T20:23:08.915",
        "kineisMetadata": {
            "sat": "KIN1B",
            "mod": "LDA2",
            "level": -119.8,
            "snr": 41.2,
            "freq": 401679400.635,
        },
        "rawData": "208d450000000000ff000000000000bb8d310000000000ff4f000000000079",
        "bitLength": 248,
        "dopplerLocId": 441,
        "dopplerRevision": 1,
        "dopplerDatetime": "2026-03-10T19:48:46.381",
        "dopplerAcqDatetime": "2026-03-10T20:23:44.217",
        "dopplerLocLon": 173.63476,
        "dopplerLocLat": -42.47563,
        "dopplerLocAlt": 0.0,
        "dopplerLocErrorRadius": 3154.0,
        "dopplerDeviceFrequency": 401677699.616,
        "dopplerLocClass": "B",
        "dopplerNbMsg": 2,
    }
