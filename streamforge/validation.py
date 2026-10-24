"""Validate traffic events against schemas/traffic_event_v1.json required fields."""

from __future__ import annotations

import json
from pathlib import Path

_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "traffic_event_v1.json"
_REQUIRED = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))["required"]


def required_fields() -> list[str]:
    return list(_REQUIRED)


def validate_event(event: dict) -> None:
    missing = [k for k in _REQUIRED if k not in event]
    if missing:
        raise ValueError(f"missing fields: {missing}")
    if event.get("schemaVersion") != "1.0.0":
        raise ValueError("unsupported schemaVersion")
    if event.get("sourceKind") not in {"SIMULATED", "OBSERVED", "DERIVED", "PREDICTED"}:
        raise ValueError("invalid sourceKind")
