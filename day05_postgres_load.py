# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5 — Postgres Serving Layer
# MAGIC
# MAGIC **Project:** merchant-campaign-pipeline
# MAGIC **Reads:**  `gold.dim_merchant`, `gold.dim_market`, `gold.fact_campaign_daily` (Databricks)
# MAGIC **Writes:** `dim_merchant`, `dim_market`, `fact_campaign_daily` (Neon Postgres)
# MAGIC
# MAGIC ### Design decisions this notebook demonstrates (interview talking points)
# MAGIC 1. **Analytical store vs. serving store.** Databricks/Delta is optimized for large
# MAGIC    scans and batch transformation (OLAP-shaped). Postgres here plays the OLTP-ish
# MAGIC    "serving" role: fast point lookups for a future FastAPI endpoint, without every
# MAGIC    request needing to spin up a Spark job. This script is the bridge between them —
# MAGIC    a batch "publish" step, not a live query pass-through.
# MAGIC 2. **Idempotent upsert, not append.** Day 2 left the rejected-table append as a
# MAGIC    known non-idempotent gap. This script does NOT repeat that mistake: every write
# MAGIC    here uses `INSERT ... ON CONFLICT DO UPDATE`, so re-running this notebook after
# MAGIC    Bronze/Silver/Gold regenerate fresh synthetic data will not duplicate rows —
# MAGIC    it corrects/replaces them in place, keyed on natural keys.
# MAGIC 3. **Small data movement, not a full re-architecture.** We're moving a few thousand
# MAGIC    aggregated rows, not raw events — this is a realistic size for a serving layer
# MAGIC    (dashboards/APIs query aggregates, not raw Bronze-scale data).

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC **Environment note — this script must run locally, not in the browser
# MAGIC notebook.** `databricks.connect`'s `DatabricksSession` is built for an
# MAGIC *external* client (your laptop) opening a new remote connection into a
# MAGIC cluster. A Databricks-hosted notebook already has its own `spark` session
# MAGIC running when it starts — importing `databricks.connect` and calling
# MAGIC `DatabricksSession.builder.getOrCreate()` from *inside* that same notebook
# MAGIC tries to bootstrap a second, redundant Spark Connect client on top of an
# MAGIC already-initialized one, which is a genuine conflict — it crashes the
# MAGIC Python kernel outright (SIGABRT) rather than failing with a normal error.
# MAGIC Run this one as `python day05_postgres_load.py` from your terminal, using
# MAGIC the project's own `.venv` (where `sqlalchemy`/`psycopg2-binary` are already
# MAGIC installed via `uv add`) — same as how Days 1–3 were run locally.

# COMMAND ----------

import config
print(config.describe())

from databricks.connect import DatabricksSession
from sqlalchemy import create_engine, text

spark = DatabricksSession.builder.getOrCreate()

# Engine created here, explicitly, at the point of use — not inside config.py.
print(f"Connecting to Postgres at {config.NEON_DATABASE_URL}")
pg_engine = create_engine(config.NEON_DATABASE_URL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Postgres tables (if not already present)
# MAGIC
# MAGIC Mirrors the Gold star schema. Primary keys here are what make the upcoming
# MAGIC `ON CONFLICT` upserts possible — Postgres needs a uniqueness constraint to
# MAGIC know what counts as "the same row" on a re-run.

# COMMAND ----------

DDL = """
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_key    INTEGER PRIMARY KEY,
    merchant_id     TEXT NOT NULL UNIQUE,
    effective_date  DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_market (
    market_key      INTEGER PRIMARY KEY,
    market          TEXT NOT NULL UNIQUE,
    effective_date  DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_campaign_daily (
    merchant_key    INTEGER NOT NULL REFERENCES dim_merchant(merchant_key),
    market_key      INTEGER NOT NULL REFERENCES dim_market(market_key),
    campaign_id     TEXT NOT NULL,
    date            DATE NOT NULL,
    spend           DOUBLE PRECISION,
    impressions     BIGINT,
    clicks          BIGINT,
    ctr             DOUBLE PRECISION,
    cost_per_click  DOUBLE PRECISION,
    PRIMARY KEY (merchant_key, market_key, campaign_id, date)
);
"""

with pg_engine.begin() as conn:
    conn.execute(text(DDL))

print("Postgres tables ensured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Gold tables from Databricks into pandas
# MAGIC
# MAGIC `.toPandas()` pulls the (small, aggregated) Gold tables back to the driver —
# MAGIC this is exactly the kind of `.collect()`-shaped action that would be a red
# MAGIC flag on Bronze-scale data (Day 1's 20,000 raw rows), but is the right tool
# MAGIC here: Gold's fact table is already aggregated down to a size a serving-layer
# MAGIC load script can reasonably hold in memory.

# COMMAND ----------

dim_merchant_pd = spark.table(config.GOLD_DIM_MERCHANT_TABLE).toPandas()
dim_market_pd = spark.table(config.GOLD_DIM_MARKET_TABLE).toPandas()
fact_pd = spark.table(config.GOLD_FACT_CAMPAIGN_DAILY_TABLE).toPandas()

print(f"dim_merchant: {len(dim_merchant_pd)} rows")
print(f"dim_market: {len(dim_market_pd)} rows")
print(f"fact_campaign_daily: {len(fact_pd)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Idempotent upsert helper
# MAGIC
# MAGIC One generic function, reused for all three tables — builds a parameterized
# MAGIC `INSERT ... ON CONFLICT (<key columns>) DO UPDATE SET ...` statement and
# MAGIC executes it inside a single transaction. Parameterized (not string-formatted)
# MAGIC to avoid SQL injection, even though this is trusted internal data — the habit
# MAGIC matters more than the specific risk here.

# COMMAND ----------

def upsert_dataframe(df, table_name: str, key_columns: list[str]):
    if df.empty:
        print(f"{table_name}: no rows to upsert, skipping.")
        return

    columns = list(df.columns)
    update_columns = [c for c in columns if c not in key_columns]

    insert_cols = ", ".join(columns)
    print(f"insert_cols = {insert_cols}")
    insert_placeholders = ", ".join(f":{c}" for c in columns)
    print(f"insert_placeholders = {insert_placeholders}")
    conflict_cols = ", ".join(key_columns)
    print(f"conflict_cols = {conflict_cols}")
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
    print(f"update_clause = {update_clause}")

    if update_clause:
        stmt = text(
            f"INSERT INTO {table_name} ({insert_cols}) "
            f"VALUES ({insert_placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
        )
    else:
        # Pure dimension with no non-key columns to update — just ignore conflicts.
        stmt = text(
            f"INSERT INTO {table_name} ({insert_cols}) "
            f"VALUES ({insert_placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        )

    records = df.to_dict(orient="records")
    print(f"statement = {stmt}")
    with pg_engine.begin() as conn:
        conn.execute(stmt, records)

    print(f"{table_name}: upserted {len(records)} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run the upserts — dimensions first, fact last
# MAGIC
# MAGIC Same dependency ordering dbt enforced automatically via `ref()` on Day 4 —
# MAGIC here it's manual, since this is plain Python, not a DAG-aware tool. The
# MAGIC foreign key constraints on `fact_campaign_daily` would fail loudly if the
# MAGIC dimensions weren't loaded first, which is a useful, explicit safety net.

# COMMAND ----------

upsert_dataframe(dim_merchant_pd, "dim_merchant", key_columns=["merchant_key"])
upsert_dataframe(dim_market_pd, "dim_market", key_columns=["market_key"])
upsert_dataframe(
    fact_pd,
    "fact_campaign_daily",
    key_columns=["merchant_key", "market_key", "campaign_id", "date"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sanity check — query Postgres directly

# COMMAND ----------

with pg_engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM fact_campaign_daily"))
    print(f"fact_campaign_daily row count in Postgres: {result.scalar()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview prep notes (read after running)
# MAGIC
# MAGIC - **"Why Postgres at all, if Databricks can already be queried?"** — separation
# MAGIC   of concerns and blast radius: a serving API shouldn't depend on a Spark
# MAGIC   cluster/warehouse being warm, and shouldn't risk an ad-hoc analytical query
# MAGIC   competing for the same compute as production API traffic.
# MAGIC - **"Why ON CONFLICT instead of TRUNCATE + INSERT?"** — TRUNCATE+INSERT would
# MAGIC   also be idempotent, but it means a brief window where the table is empty
# MAGIC   mid-load — a request hitting the API during that window sees no data. Upsert
# MAGIC   keeps existing rows queryable throughout the load.
# MAGIC - **"What guarantees no duplicates on a re-run?"** — the PRIMARY KEY /
# MAGIC   UNIQUE constraints defined in the DDL. Without those, ON CONFLICT has
# MAGIC   nothing to match against and this degrades back to plain (non-idempotent)
# MAGIC   INSERT — the constraint is doing the real work, not the SQL keyword alone.
