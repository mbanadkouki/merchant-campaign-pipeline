# merchant-campaign-pipeline

An end-to-end data engineering pipeline simulating how an advertising
platform turns raw merchant campaign events into reliable, decision-ready
metrics — built as hands-on preparation for a Senior Data Engineer role,
mirroring the modern Berlin data stack (Databricks, Delta Lake, dbt,
Airflow, Postgres).

**Business scenario:** ingest raw campaign event data (impressions,
clicks, spend) across five markets and fifty merchants, clean and model
it through a Medallion architecture (Bronze → Silver → Gold), serve
trustworthy daily performance metrics (CTR, cost-per-click) through a
fast serving layer, and orchestrate the whole thing end-to-end,
unattended.

## Architecture

```
Synthetic data
      │
      ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Bronze    │───▶│   Silver    │───▶│    Gold     │───▶│  Postgres   │
│  (raw, as-  │    │ (deduped,   │    │ (star schema│    │  serving    │
│  ingested)  │    │  validated) │    │  + metrics) │    │   layer     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
  PySpark /           PySpark /          PySpark AND        idempotent
  Delta Lake          Delta Lake         dbt (parallel       upsert via
                       + quarantine       implementations)    SQLAlchemy
                       table                                  or direct
                                                                Spark JDBC

All orchestrated end-to-end by Apache Airflow (Docker/WSL2).
```

- **Bronze**: synthetic merchant campaign events, ingested as-is into
  Delta Lake on Databricks (serverless compute, Unity Catalog).
- **Silver**: deduplicated via window functions (latest `ingestion_ts`
  wins on business-key collisions), schema-enforced, business-rule
  validated. Invalid rows are **quarantined** to a `_rejected` table
  with a reason code — never silently dropped.
- **Gold**: a star schema (`dim_merchant`, `dim_market`,
  `fact_campaign_daily`) with surrogate keys, null-safe derived metrics
  (CTR, cost-per-click), built **twice** — once in PySpark, once in dbt
  — deliberately, to compare the two approaches (see [Known
  trade-offs](#known-trade-offs) below).
- **Serving layer**: Gold's tables are batch-loaded into Postgres
  (Neon) via idempotent `INSERT ... ON CONFLICT DO UPDATE` upserts, so
  the pipeline can be safely re-run without duplicating data. Two
  implementations exist: one that pulls data through the driver via
  `.toPandas()`, and one that writes directly from Spark executors to
  Postgres in parallel — see [`day05_postgres_load.py`](day05_postgres_load.py)
  vs [`day05_PostgresLoadDirectlyFromDatabricks.py`](day05_PostgresLoadDirectlyFromDatabricks.py).
- **Orchestration**: a single Airflow DAG chains all five stages with
  strict dependency ordering — if any stage fails, everything
  downstream is skipped rather than running against stale data.

## Stack

| Layer | Tool |
|---|---|
| Compute / storage | Databricks Free Edition (serverless), Unity Catalog, Delta Lake |
| Transformation | PySpark, dbt (dbt-core + dbt-databricks) |
| Serving | Postgres (Neon) |
| Orchestration | Apache Airflow (Docker Compose) |
| Local dev | VSCode + Databricks Connect, `uv` for package management |

## Repository structure

```
config/                   Environment config (dev/staging/prod YAML)
dbt/merchant_campaign_dbt/  dbt project — Gold layer as declarative SQL models
airflow/                  Airflow DAG + Docker Compose setup
day01_bronze_ingestion.py
day02_silver_transformation.py
day03_gold_layer.py       Gold layer, PySpark implementation
day05_postgres_load.py    Postgres load via .toPandas()
day05_PostgresLoadDirectlyFromDatabricks.py  Postgres load via direct Spark JDBC write
config.py                 Single import seam for all environment config
```

(dbt's own Gold implementation lives inside `dbt/merchant_campaign_dbt/models/marts/`.)

## Running it

1. **Local setup**: `uv sync` to create the venv (Databricks Connect,
   dbt, SQLAlchemy, psycopg2). Copy `.env.example` to `.env` and fill in
   your Databricks and Neon credentials.
2. **Run a single stage**: `python day01_bronze_ingestion.py` (and so
   on through `day05`). Each stage reads from the previous one's output
   table and is independently idempotent.
3. **Run the dbt Gold build**: `cd dbt/merchant_campaign_dbt && dbt run && dbt test`.
4. **Run the full pipeline via Airflow**: `cd airflow && docker compose up`,
   then trigger the `merchant_campaign_pipeline` DAG from
   `http://localhost:8080`.

## Known trade-offs

Documented deliberately, not hidden — these are real design decisions
made during the build, with the reasoning behind each:

- **Gold is built by both PySpark and dbt.** Kept as-is to preserve the
  comparison between the two approaches; a real team would pick one
  owner for this schema, not run both against the same tables.
- **The Silver rejected/quarantine table uses append-mode writes**,
  which is not strictly idempotent on reruns of failed batches — a
  known, accepted trade-off given the low practical impact for this
  dataset's scale.
- **`_PIP_ADDITIONAL_REQUIREMENTS` is used to install dependencies into
  the Airflow containers** — Apache's own documentation flags this as a
  dev/test-only convenience, not a production pattern. A production
  deployment would use a custom-built Airflow image instead.
- **Databricks Free Edition is serverless-only**, which surfaces real,
  current platform constraints not covered by most tutorials: no
  `.rdd` API access (Spark Connect doesn't expose it), and writes to
  external databases restricted to a curated set of native connectors
  rather than the fully generic JDBC format.

## What this project demonstrates

- Medallion architecture with genuine data-quality gates (schema
  enforcement, quarantine, not silent drops)
- Star schema design with surrogate keys and SCD Type 1 (Type-2-shaped
  for future extension)
- The same transformation logic implemented in two paradigms (PySpark
  vs. declarative SQL/dbt) for direct comparison
- Idempotent, re-runnable pipeline design end-to-end
- Real debugging of current, version-specific platform constraints —
  Spark Connect's execution model, serverless compute restrictions, and
  Databricks Connect's local/remote execution split
- Full orchestration with dependency-aware failure handling