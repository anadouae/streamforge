#!/usr/bin/env python3
"""StreamForge producer — publishes validated SIMULATED traffic events to Kafka."""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from kafka import KafkaProducer

REQUIRED = json.loads((Path(__file__).resolve().parents[1] / "schemas/traffic_event_v1.json").read_text())[
    "required"
]
SEGMENTS = [f"sf-seg-{i}" for i in range(1, 9)]


def validate(event: dict) -> None:
    missing = [k for k in REQUIRED if k not in event]
    if missing:
        raise ValueError(f"missing fields: {missing}")
    if event["schemaVersion"] != "1.0.0":
        raise ValueError("unsupported schemaVersion")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="127.0.0.1:9092")
    p.add_argument("--topic", default="streamforge.traffic.events")
    p.add_argument("--rounds", type=int, default=3)
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        acks="all",
    )

    # ensure topic exists by sending (Urban Pulse create-topics style — broker auto-create ok for demo)
    sent = 0
    for r in range(args.rounds):
        for seg in SEGMENTS:
            rush = 0.5 + 0.5 * math.sin(r + hash(seg) % 7)
            event = {
                "eventId": str(uuid4()),
                "eventType": "traffic.measurement",
                "eventTimestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "sourceId": "streamforge-producer",
                "spatialEntityId": seg,
                "schemaVersion": "1.0.0",
                "measurementName": "speed_kmh",
                "measurementValue": round(15 + 40 * (1 - rush * 0.6), 2),
                "measurementUnit": "km/h",
                "qualityStatus": "ok",
                "sourceKind": "SIMULATED",
            }
            validate(event)
            producer.send(args.topic, key=seg, value=event)
            sent += 1
            time.sleep(0.01)
    producer.flush()
    producer.close()
    print(f"StreamForge published {sent} validated events -> {args.topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
