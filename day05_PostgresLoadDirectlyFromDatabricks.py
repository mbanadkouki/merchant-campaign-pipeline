# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5 (alt) — Postgres Serving Layer via Direct Executor Write
# MAGIC
# MAGIC **Project:** merchant-campaign-pipeline
# MAGIC **Contrast with `day05_postgres_load.py`:** that version used `.toPandas()`,
# MAGIC which pulls every row through the driver (your laptop) before writing to
# MAGIC Postgres. This version uses Spark's own JDBC writer, so each **executor**
# MAGIC (running on the Databricks cluster) writes its own slice of data **directly**
# MAGIC to Postgres, in parallel — your laptop never sees the actual rows, only
# MAGIC coordinates the job. This is the pattern that scales to Bronze-sized data;
# MAGIC `.toPandas()` does not.
# MAGIC
# MAGIC ### The one thing direct JDBC write can't do: upsert
# MAGIC Spark's JDBC writer only supports `overwrite` / `append` / `ignore` / `error`
# MAGIC modes — there's no `ON CONFLICT DO UPDATE` equivalent, because that's a
# MAGIC row-by-row database concept and JDBC writes are a distributed bulk operation
# MAGIC across many parallel connections. The standard production pattern (used here):
# MAGIC 1. Executors write the full DataFrame to a **staging table** (`mode="overwrite"`)
# MAGIC    — this is the part that happens directly, in parallel, bypassing the driver.
# MAGIC 2. **One single SQL statement** — `INSERT ... SELECT ... FROM staging
# MAGIC    ON CONFLICT DO UPDATE` — runs once, from the driver, moving staging → final
# MAGIC    table inside Postgres itself. This is a lightweight control-plane statement,
# MAGIC    not a bulk data transfer — the actual row data never re-crosses the network.

# COMMAND ----------

import config
print(config.describe())

from urllib.parse import urlparse
from databricks.connect import DatabricksSession
from sqlalchemy import create_engine, text

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parse NEON_DATABASE_URL into connection components
# MAGIC
# MAGIC `NEON_DATABASE_URL` is a `postgresql://user:pass@host/db` URI — the format
# MAGIC SQLAlchemy/psycopg2 expect. Serverless compute's native `postgresql` write
# MAGIC connector wants these as **separate options** (`host`, `port`, `database`,
# MAGIC `user`, `password`) rather than one URL string — this parsing step exists
# MAGIC purely to bridge that difference. Same credentials, different shape.
# MAGIC
# MAGIC **Why not the generic `jdbc` format?** Serverless compute only allows DML
# MAGIC through a curated list of native connectors (`postgresql`, `mysql`,
# MAGIC `snowflake`, `delta`, etc.) — the fully generic `jdbc` format (with an
# MAGIC arbitrary driver class string) isn't on that allow-list and gets rejected
# MAGIC with `UNSUPPORTED_DATA_SOURCE_WRITE`. The native connector is what
# MAGIC serverless actually wants here.

# COMMAND ----------

parsed = urlparse(config.NEON_DATABASE_URL)

pg_host = parsed.hostname
pg_port = parsed.port or 5432
pg_database = parsed.path.lstrip("/")
pg_user = parsed.username
pg_password = parsed.password

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ensure staging + final tables exist
# MAGIC
# MAGIC Staging tables are unconstrained (no primary key) — they're overwritten
# MAGIC wholesale every run, so uniqueness doesn't need enforcing there. The final
# MAGIC tables keep the same PRIMARY KEY/UNIQUE constraints as the .toPandas()
# MAGIC version, since those constraints are what make `ON CONFLICT` meaningful.

# COMMAND ----------

pg_engine = create_engine(config.NEON_DATABASE_URL)

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

CREATE TABLE IF NOT EXISTS staging_dim_merchant (LIKE dim_merchant INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS staging_dim_market (LIKE dim_market INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS staging_fact_campaign_daily (LIKE fact_campaign_daily INCLUDING DEFAULTS);
"""

with pg_engine.begin() as conn:
    conn.execute(text(DDL))

print("Postgres tables (final + staging) ensured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Direct executor-to-Postgres write into staging tables
# MAGIC
# MAGIC THIS is the step that never touches the driver/laptop for actual data —
# MAGIC each partition of each DataFrame is written by whichever executor holds
# MAGIC it, straight to Postgres, in parallel. Compare this to Day 5's
# MAGIC `.toPandas()` line, which pulled everything through the driver first.

# COMMAND ----------

def write_staging(df, staging_table: str):
    (
        df.write
        .format("postgresql")
        .option("host", pg_host)
        .option("port", str(pg_port))
        .option("database", pg_database)
        .option("dbtable", staging_table)
        .option("user", pg_user)
        .option("password", pg_password)
        .mode("overwrite")
        .option("truncate", "true")   # TRUNCATE + reload, not DROP/CREATE — keeps table structure/perms intact
        .save()
    )

dim_merchant_df = spark.table(config.GOLD_DIM_MERCHANT_TABLE)
dim_market_df = spark.table(config.GOLD_DIM_MARKET_TABLE)
fact_df = spark.table(config.GOLD_FACT_CAMPAIGN_DAILY_TABLE)

write_staging(dim_merchant_df, "staging_dim_merchant")
write_staging(dim_market_df, "staging_dim_market")
write_staging(fact_df, "staging_fact_campaign_daily")

print("Staging tables loaded directly from executors.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Single upsert statement: staging → final
# MAGIC
# MAGIC This runs from the driver — but it's one lightweight SQL command per
# MAGIC table, not a bulk data transfer. The actual row data already arrived at
# MAGIC Postgres in step 3; this step just tells Postgres to reconcile two of its
# MAGIC own tables against each other, entirely server-side.

# COMMAND ----------

UPSERT_STATEMENTS = [
    """
    INSERT INTO dim_merchant (merchant_key, merchant_id, effective_date)
    SELECT merchant_key, merchant_id, effective_date FROM staging_dim_merchant
    ON CONFLICT (merchant_key) DO UPDATE SET
        merchant_id = EXCLUDED.merchant_id,
        effective_date = EXCLUDED.effective_date;
    """,
    """
    INSERT INTO dim_market (market_key, market, effective_date)
    SELECT market_key, market, effective_date FROM staging_dim_market
    ON CONFLICT (market_key) DO UPDATE SET
        market = EXCLUDED.market,
        effective_date = EXCLUDED.effective_date;
    """,
    """
    INSERT INTO fact_campaign_daily
        (merchant_key, market_key, campaign_id, date, spend, impressions, clicks, ctr, cost_per_click)
    SELECT
        merchant_key, market_key, campaign_id, date, spend, impressions, clicks, ctr, cost_per_click
    FROM staging_fact_campaign_daily
    ON CONFLICT (merchant_key, market_key, campaign_id, date) DO UPDATE SET
        spend = EXCLUDED.spend,
        impressions = EXCLUDED.impressions,
        clicks = EXCLUDED.clicks,
        ctr = EXCLUDED.ctr,
        cost_per_click = EXCLUDED.cost_per_click;
    """,
]

with pg_engine.begin() as conn:
    for stmt in UPSERT_STATEMENTS:
        conn.execute(text(stmt))

print("Staging \u2192 final upsert complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sanity check

# COMMAND ----------

with pg_engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM fact_campaign_daily"))
    print(f"fact_campaign_daily row count in Postgres: {result.scalar()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview prep notes (read after running)
# MAGIC
# MAGIC - **"Why not just JDBC-write straight into the final table?"** — because JDBC
# MAGIC   write modes don't support upsert. Writing straight to the final table would
# MAGIC   force `overwrite` (destroys history/other data momentarily) or `append`
# MAGIC   (duplicates on re-run) — neither is idempotent. Staging + a single upsert
# MAGIC   statement gets you both distributed writes AND idempotency.
# MAGIC - **"Why format('postgresql') instead of format('jdbc')?"** — serverless
# MAGIC   compute only permits DML through a curated allow-list of native connectors
# MAGIC   (`postgresql`, `mysql`, `snowflake`, `delta`, and a handful of others).
# MAGIC   The fully generic `jdbc` format — pointing at an arbitrary driver class —
# MAGIC   isn't on that list and is rejected outright with
# MAGIC   `UNSUPPORTED_DATA_SOURCE_WRITE`, regardless of whether the driver itself
# MAGIC   would have worked. This is a serverless-specific restriction; classic
# MAGIC   clusters allow the generic `jdbc` format with any installed driver.
# MAGIC - **"What actually moved through the driver here?"** — only SQL text and a
# MAGIC   scalar row count (step 5), plus the job coordination itself. Every actual
# MAGIC   data row went executor \u2192 Postgres directly in step 3.