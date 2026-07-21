# E-Commerce Analytics Platform

A portfolio-grade, end-to-end streaming data engineering platform demonstrating production patterns across ingestion, processing, orchestration, and cloud storage — built to reflect real-world architecture decisions rather than toy examples.

## Architecture Overview

```
Kafka Topics (Redpanda Cloud)
        │
        ▼
   Bronze Layer  (raw ingestion, Avro decode)
        │
        ▼
   Silver Layer  (dedup, cleaning, MERGE)
        │
        ▼
   Gold Layer    (windowed aggregations, business metrics)
        │
        ▼
  AWS S3 (Delta Lake, Unity Catalog External Location)
        │
        ▼
  Orchestrated by Airflow (Docker) → Databricks Jobs
```

Three independent data domains — **user activity**, **product events**, and **order events** — each flow through their own Bronze → Silver → Gold pipeline, using a single generic, config-driven codebase rather than per-topic notebooks.

## Data Sources

Three Kafka topics, produced by independent Dockerized microservices, ingested via Redpanda Cloud:

| Topic | Producer | Rate | Description |
|---|---|---|---|
| `user-activity` | `user-activity-producer` | 10 events/sec | Browsing behavior: page views, searches, cart actions, checkout, payment |
| `product-events` | `product-event-producer` | 5 events/sec | Catalog changes: creation, updates, price changes, stock changes |
| `order-events` | `order-event-producer` | 3 events/sec | Order lifecycle: placed, confirmed, shipped, delivered, cancelled |

Each topic is defined by an Avro schema and validated through a schema registry (Redpanda Cloud), ensuring producers and consumers stay in sync as the schema evolves.

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming ingestion | Apache Kafka (via Redpanda Cloud), Avro, Schema Registry |
| Processing | PySpark Structured Streaming on Databricks Free Edition |
| Storage | Delta Lake, AWS S3 (Unity Catalog External Location) |
| Orchestration | Apache Airflow (Dockerized), Databricks Jobs API |
| Secrets management | Databricks Secrets (scoped, not hardcoded) |
| Version control | Git, Databricks Repos (Git-first workflow) |

## Medallion Architecture

**Bronze** — Raw ingestion, zero transformation. Kafka messages are decoded from Avro's Confluent wire format (magic byte + schema ID header stripped, payload deserialized against the registry schema) and landed as-is into Delta tables.

**Silver** — Deduplication and cleaning. Since Kafka guarantees at-least-once delivery, the same event can be redelivered; Silver deduplicates using Delta `MERGE` inside `foreachBatch`, keyed on `event_id`.

**Gold** — Business aggregation. Windowed, watermarked streaming aggregations (hourly tumbling windows) produce metrics like funnel conversion counts, revenue rollups, and price-change summaries — the layer consumed by dashboards and downstream analytics.

## Generic, Config-Driven Pipeline Design

Rather than hardcoding a separate notebook per topic, the pipeline is built around a single configuration dictionary (`databricks/src/config.py`) defining each topic's schema, dedup keys, table names, checkpoints, and Gold aggregation specs. Three parameterized notebooks — `bronze_ingestion`, `silver_cleaning`, `gold_aggregations` — handle all three topics by accepting a `topic_name` widget parameter and dispatching to shared utility functions (`bronze_utils.py`, `silver_utils.py`, `gold_utils.py`).

Adding a new data source requires adding a config entry — not writing new notebook code.

## Orchestration

All nine tasks (3 medallion layers × 3 topics) run as a single, sequentially-chained Databricks Job, triggered every 15 minutes by an Airflow DAG (`DatabricksRunNowOperator`) running in Docker. Each layer uses Spark's `availableNow` micro-batch trigger — processing all currently available data, then stopping — rather than a continuously-running stream. This is both a deliberate cost/latency tradeoff and, on Databricks Free Edition specifically, the only supported trigger mode (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` blocks continuous triggers on this tier).

## Storage: AWS S3 via Unity Catalog External Location

Delta tables are stored in a customer-owned S3 bucket rather than Databricks' default managed storage, registered as a Unity Catalog External Location via the Databricks AWS Quickstart (CloudFormation-backed, auto-provisioning the IAM role and Storage Credential). This reflects a production-realistic pattern where the organization retains ownership and control of underlying storage.

## Key Engineering Challenges Solved

- **Delta streaming checkpoint fragility** — Dropping and recreating a Delta table generates a new internal table UUID, invalidating every downstream streaming checkpoint (`DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`). Resolved by preferring `ALTER TABLE`/schema evolution over drop-and-recreate on tables with active downstream consumers, and by establishing a clean checkpoint-reset procedure for legitimate migrations (e.g., the S3 storage migration).
- **Kafka retention vs. consumer lag** — If Bronze falls behind Redpanda's retention window, expected offsets age out (`OffsetOutOfRangeException`). Handled via `failOnDataLoss=false`, a standard production tradeoff between strict consistency and pipeline availability.
- **Generic pipeline refactor** — Migrated from three hardcoded, duplicated notebooks to one config-driven system, cutting future onboarding of new topics down to a config entry.
- **Free-tier orchestration constraints** — Free Edition enforces a low concurrent-run limit; the job graph was redesigned as a sequential chain rather than parallel per-topic branches to operate reliably within this constraint.

## Local Development

```bash
# Start producers, Airflow, and supporting infra
docker compose up -d

# Airflow UI
http://localhost:8080

# Databricks workspace
https://dbc-1e693bd2-dc7b.cloud.databricks.com
```

## Roadmap

- [x] Kafka ingestion via Redpanda Cloud (3 topics)
- [x] Generic, config-driven Bronze → Silver → Gold pipeline
- [x] Databricks Job orchestration (9-task chain)
- [x] Airflow scheduling (Dockerized)
- [x] AWS S3 storage migration via Unity Catalog External Location
- [ ] RAG/GenAI query layer (ChromaDB + FastAPI + LLM generation)
- [ ] Monitoring dashboard (Prometheus/Grafana)
