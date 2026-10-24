# STREAMFORGE — Real-Time Big Data Intelligence Platform

Reusable event platform: **Kafka topics**, schema validation, Python producers/consumers, quality metrics, and a path to Spark/Postgres.

Works with Urban Pulse topic `urban.traffic.events` (or its own demo topic).

## Quick start

Requires Kafka at `127.0.0.1:9092`.

```bash
python3 -m pip install -r requirements.txt
export PYTHONPATH=.
pytest -q
python3 producers/traffic_producer.py --rounds 5
python3 consumers/quality_consumer.py --max-messages 100
```

See `SECURITY.md` and `docs/PROJECT_METHODOLOGY.md`. CI + Dockerfile included. Use `kafka-python-ng` (Py3.12).

## Layout

```text
producers/     # schema-validating producers
consumers/     # quality + lag-oriented consumers
schemas/       # event contracts
docs/          # architecture notes
```

## Author

Douae El Assal · https://github.com/anadouae
