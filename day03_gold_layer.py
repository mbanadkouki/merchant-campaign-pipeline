# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Day 3 — Gold Layer: Star Schema (Dimensions + Fact)
# MAGIC
# MAGIC **Project:** merchant-campaign-pipeline
# MAGIC **Reads:**  `silver.merchant_campaign_events` (from Day 2)
# MAGIC **Writes:** `gold.dim_merchant`, `gold.dim_market`, `gold.fact_campaign_daily`
# MAGIC
# MAGIC ### Design decisions this notebook demonstrates (interview talking points)
# MAGIC 1. **Star schema, not just "more aggregation."** Silver is flat and grain-preserving.
# MAGIC    Gold reshapes into dimensions (who/where) + a fact (what happened), joined by
# MAGIC    surrogate keys instead of repeating natural keys (merchant_id, market strings)
# MAGIC    everywhere. This is what makes Gold "decision-ready" rather than just "cleaner Silver."
# MAGIC 2. **Surrogate keys, generated here — not the natural key.** `merchant_id` /
# MAGIC    `market` remain as attributes on the dimension, but `merchant_key` / `market_key`
# MAGIC    (small ints) are what the fact table actually references. This is standard star-schema
# MAGIC    practice and is what makes SCD Type 2 possible later without breaking every fact row.
# MAGIC 3. **SCD Type 1 today, Type-2-shaped for tomorrow.** Dimensions are overwritten in
# MAGIC    full each run (Type 1 — simple, no history). But `effective_date` is included now,
# MAGIC    so if this ever needs real history tracking (Type 2: insert new row + close out old
# MAGIC    row instead of overwriting), the schema doesn't need to change, only the write logic.
# MAGIC 4. **Derived business metrics computed once, here** (`ctr`, `cost_per_click`) — so every
# MAGIC    downstream consumer (Postgres, FastAPI, a BI tool) gets the same numbers instead of
# MAGIC    each recomputing them slightly differently.

# COMMAND ----------

import config
print(config.describe())

from pyspark.sql import functions as F
from databricks.connect import DatabricksSession
from pyspark.sql.window import Window

spark = DatabricksSession.builder.getOrCreate()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.GOLD_SCHEMA}")

SILVER_TABLE = config.SILVER_EVENTS_TABLE
DIM_MERCHANT_TABLE = config.GOLD_DIM_MERCHANT_TABLE
DIM_MARKET_TABLE = config.GOLD_DIM_MARKET_TABLE
FACT_TABLE = config.GOLD_FACT_CAMPAIGN_DAILY_TABLE

silver_df = spark.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build `dim_merchant`
# MAGIC
# MAGIC One row per distinct `merchant_id` seen in Silver. `merchant_key` is a surrogate
# MAGIC key generated here via `row_number()` — deterministic given a fixed input, which
# MAGIC matters because Bronze regenerates random data each run, so re-running this
# MAGIC notebook against a fresh Bronze/Silver run can validly produce different keys.
# MAGIC That's expected for a dev/synthetic dataset; in a real Type-2 dimension, keys
# MAGIC would persist across runs by looking up existing rows first, not regenerating
# MAGIC from scratch every time — flagged here as the difference between what we're
# MAGIC doing (Type 1, full rebuild) and what Type 2 would require.

# COMMAND ----------

merchant_window = Window.orderBy("merchant_id")

dim_merchant_df = (
    silver_df
    .select("merchant_id")
    .distinct()
    .withColumn("merchant_key", F.row_number().over(merchant_window))
    .withColumn("effective_date", F.current_date())
    .select("merchant_key", "merchant_id", "effective_date")
)
# SCD (Slowly Changing Dimension) type 1 used here: overwrite the entire dimension each run, no history kept. 
# Type 2 would require a more complex merge logic to preserve historical rows and only insert new ones or update existing ones with an end date.
(
    dim_merchant_df.write
    .format("delta")
    .mode("overwrite")   # SCD Type 1: full rebuild, no history kept 
    .option("mergeSchema", "false")
    .saveAsTable(DIM_MERCHANT_TABLE)
)

print(f"dim_merchant rows: {dim_merchant_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build `dim_market`
# MAGIC
# MAGIC Same pattern, much smaller cardinality (5 markets). Small dimensions like this
# MAGIC are sometimes left as plain strings directly on the fact table in real systems
# MAGIC (not worth a join for 5 values) — built as a proper dimension here anyway since
# MAGIC the point of this exercise is demonstrating the pattern, not micro-optimizing
# MAGIC a 5-row table.

# COMMAND ----------

market_window = Window.orderBy("market")

dim_market_df = (
    silver_df
    .select("market")
    .distinct()
    .withColumn("market_key", F.row_number().over(market_window))
    .withColumn("effective_date", F.current_date())
    .select("market_key", "market", "effective_date")
)

(
    dim_market_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "false")
    .saveAsTable(DIM_MARKET_TABLE)
)

print(f"dim_market rows: {dim_market_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build `fact_campaign_daily`
# MAGIC
# MAGIC Same grain as Silver (`merchant_id + campaign_id + market + date`), but:
# MAGIC - natural keys replaced with dimension surrogate keys via joins
# MAGIC - derived metrics computed once: `ctr` (click-through rate) and
# MAGIC   `cost_per_click`
# MAGIC
# MAGIC **Null-safety on the derived metrics:** dividing by zero impressions/clicks
# MAGIC would otherwise produce `null` silently (Spark's default division behavior)
# MAGIC — using `F.when()` to make the zero-denominator case an explicit, visible
# MAGIC decision (0.0) rather than an unexplained null a downstream consumer has to
# MAGIC guess about.

# COMMAND ----------

fact_df = (
    silver_df
    .join(dim_merchant_df.select("merchant_key", "merchant_id"), on="merchant_id", how="inner")
    .join(dim_market_df.select("market_key", "market"), on="market", how="inner")
    .withColumn(
        "ctr", # click-through rate = clicks / impressions, null-safe
        F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions")).otherwise(F.lit(0.0))
    )
    .withColumn(
        "cost_per_click",
        F.when(F.col("clicks") > 0, F.col("spend") / F.col("clicks")).otherwise(F.lit(0.0))
    )
    .select(
        "merchant_key",
        "market_key",
        "campaign_id",
        "date",
        "spend",
        "impressions",
        "clicks",
        "ctr",
        "cost_per_click",
    )
)

(
    fact_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "false")
    .partitionBy("market_key", "date")
    .saveAsTable(FACT_TABLE)
)

print(f"fact_campaign_daily rows: {fact_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sanity checks

# COMMAND ----------

spark.table(DIM_MERCHANT_TABLE).limit(5).show(truncate=False)
spark.table(DIM_MARKET_TABLE).show(truncate=False)
spark.table(FACT_TABLE).limit(5).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview prep notes (read after running)
# MAGIC
# MAGIC - **"Why a surrogate key instead of just using merchant_id directly?"** —
# MAGIC   decouples the fact table from the natural key's format/stability, and is what
# MAGIC   makes SCD Type 2 possible later: the same merchant_id can map to multiple
# MAGIC   dimension rows (different merchant_key values) over time if attributes change,
# MAGIC   without needing to touch historical fact rows.
# MAGIC - **"Why compute ctr/cost_per_click here instead of at query time?"** —
# MAGIC   compute-once-consume-many. Every downstream consumer (Postgres, FastAPI,
# MAGIC   a dashboard) gets identical numbers instead of subtly different rounding/edge-case
# MAGIC   handling if five teams each reimplement the formula.
# MAGIC - **"Why overwrite for the dimensions instead of tracking history?"** —
# MAGIC   this is Type 1 vs Type 2 as a deliberate choice, not a limitation: Type 1 is
# MAGIC   the right default when historical accuracy isn't a business requirement.
# MAGIC   The `effective_date` column is there so the schema wouldn't need to change
# MAGIC   if that requirement showed up later — only the write logic (insert instead
# MAGIC   of overwrite, plus an `end_date`) would need to change.
