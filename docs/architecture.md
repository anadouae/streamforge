# StreamForge architecture (MVP)

```text
Producer (validate schema)
        │
        ▼
Kafka topic (partitions)
        │
        ▼
Quality consumer (metrics: ok/invalid/lag)
        │
        ▼
(later) Spark jobs → Postgres/Parquet → Java API
```

Urban Pulse can share the same event schema (`1.0.0`) for ecosystem interoperability.
