import pytest

from streamforge.validation import required_fields, validate_event


def _valid() -> dict:
    return {
        "eventId": "e1",
        "eventType": "traffic.measurement",
        "eventTimestamp": "2026-10-24T12:00:00Z",
        "sourceId": "test",
        "spatialEntityId": "sf-seg-1",
        "schemaVersion": "1.0.0",
        "measurementName": "speed_kmh",
        "measurementValue": 40.0,
        "measurementUnit": "km/h",
        "qualityStatus": "ok",
        "sourceKind": "SIMULATED",
    }


def test_required_fields_non_empty():
    assert "eventId" in required_fields()


def test_validate_ok():
    validate_event(_valid())


def test_validate_missing():
    bad = _valid()
    del bad["eventId"]
    with pytest.raises(ValueError, match="missing"):
        validate_event(bad)


def test_validate_bad_kind():
    bad = _valid()
    bad["sourceKind"] = "UNKNOWN"
    with pytest.raises(ValueError, match="sourceKind"):
        validate_event(bad)
