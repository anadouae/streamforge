# Security — StreamForge

- Events are **SIMULATED** unless `sourceKind` says otherwise.
- Schema validation rejects incomplete / invalid provenance fields.
- No credentials in repo; Kafka bootstrap via CLI flags / env only.
- Prefer network isolation: broker on localhost or private VPC.
- Dependency: `kafka-python-ng` (Py3.12-compatible). Do not pin broken `kafka-python==2.0.2` on 3.12.
