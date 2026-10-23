#!/usr/bin/env python3
"""StreamForge quality consumer — validates payloads and reports basic metrics."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

REQUIRED = json.loads((Path(__file__).resolve().parents[1] / "schemas/traffic_event_v1.json").read_text())[
    "required"
]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="127.0.0.1:9092")
    p.add_argument("--topic", default="streamforge.traffic.events")
    p.add_argument("--max-messages", type=int, default=50)
    p.add_argument("--group", default="streamforge-quality")
    args = p.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=8000,
    )

    ok = bad = 0
    t0 = time.time()
    for i, msg in enumerate(consumer):
        event = msg.value
        missing = [k for k in REQUIRED if k not in event]
        if missing or event.get("schemaVersion") != "1.0.0":
            bad += 1
        else:
            ok += 1
        if i + 1 >= args.max_messages:
            break

    # Rough lag snapshot
    lag_total = 0
    try:
        parts = consumer.partitions_for_topic(args.topic) or set()
        tps = [TopicPartition(args.topic, p) for p in parts]
        end = consumer.end_offsets(tps)
        for tp in tps:
            pos = consumer.position(tp)
            lag_total += max(0, end.get(tp, 0) - pos)
    except Exception:  # noqa: BLE001
        lag_total = -1

    elapsed = time.time() - t0
    consumer.close()
    print(
        json.dumps(
            {
                "ok": ok,
                "invalid": bad,
                "elapsed_sec": round(elapsed, 3),
                "approx_lag": lag_total,
                "topic": args.topic,
            },
            indent=2,
        )
    )
    return 0 if bad == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
