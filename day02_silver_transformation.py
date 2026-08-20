# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Day 2 — Silver Layer: Dedup, Null Handling, Schema Enforcement
# MAGIC
# MAGIC **Project:** merchant-campaign-pipeline
# MAGIC **Reads:**  `bronze.merchant_campaign_events` (from Day 1)
# MAGIC **Writes:** `silver.merchant_campaign_events` (clean, deduped, schema-enforced)
# MAGIC            `silver.merchant_campaign_events_rejected` (quarantine table)
# MAGIC
# MAGIC ### Design decisions this notebook demonstrates (interview talking points)
# MAGIC 1. **Bronze is permissive, Silver is strict.** Bronze accepted whatever the source sent.
# MAGIC    Silver is the quality gate — schema is enforced explicitly, no silent `mergeSchema`.
# MAGIC 2. **Dedup uses an explicit window function**, not `dropDuplicates()`, because we need
# MAGIC    control over *which* record wins when duplicates exist (latest by ingestion time).
# MAGIC 3. **Bad records are quarantined, never silently dropped.** A record that fails
# MAGIC    validation is written to a `_rejected` table with a reason code, so nothing
# MAGIC    disappears without a trace and downstream consumers can alert on volume.

# COMMAND ----------
import config
print(config.describe())

from pyspark.sql import functions as F, Window
from databricks.connect import DatabricksSession

from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType,
    LongType, TimestampType
)
from datetime import date as pydate

spark = DatabricksSession.builder.getOrCreate()

# Databricks Free Edition's default catalog is "workspace" — Unity Catalog
# requires the three-level catalog.schema.table name, same as Bronze.

# spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
# BRONZE_TABLE = "workspace.bronze.merchant_campaign_events"
# SILVER_TABLE = "workspace.silver.merchant_campaign_events"
# REJECTED_TABLE = "workspace.silver.merchant_campaign_events_rejected"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.SILVER_SCHEMA}")
BRONZE_TABLE = config.BRONZE_EVENTS_TABLE
SILVER_TABLE = config.SILVER_EVENTS_TABLE
REJECTED_TABLE = config.SILVER_REJECTED_TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explicit Silver schema
# MAGIC
# MAGIC Bronze schema was inferred / permissive. Silver defines the contract explicitly.
# MAGIC This is what "schema-on-write" looks like in practice — if upstream sends a
# MAGIC column of the wrong type, this cast step is where it fails loudly instead of
# MAGIC quietly corrupting downstream aggregates.

# COMMAND ----------

silver_schema = StructType([
    StructField("merchant_id",   StringType(),    False),
    StructField("campaign_id",   StringType(),    False),
    StructField("market",        StringType(),    False),
    StructField("date",          DateType(),      False),
    StructField("spend",         DoubleType(),    True),
    StructField("impressions",   LongType(),      True),
    StructField("clicks",        LongType(),      True),
    StructField("ingestion_ts",  TimestampType(), False),
])

bronze_df = spark.table(BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cast + validate
# MAGIC
# MAGIC We cast defensively (`.cast()` returns null on failure rather than throwing),
# MAGIC then explicitly flag rows that fail business rules. This separates "type
# MAGIC casting failed" from "value is semantically invalid" — two different failure
# MAGIC modes worth distinguishing in an interview.

# COMMAND ----------

typed_df = (
    bronze_df
    .withColumn("merchant_id", F.col("merchant_id").cast(StringType()))
    .withColumn("campaign_id", F.col("campaign_id").cast(StringType()))
    .withColumn("market",      F.col("market").cast(StringType()))
    .withColumn("date",        F.col("date").cast(DateType()))
    .withColumn("spend",       F.col("spend").cast(DoubleType()))
    .withColumn("impressions", F.col("impressions").cast(LongType()))
    .withColumn("clicks",      F.col("clicks").cast(LongType()))
    .withColumn("ingestion_ts", F.col("ingestion_ts").cast(TimestampType()))
)

# Business validation rules — each produces a reason code if violated.
# Semantic nulls (e.g. spend missing on an active campaign) are treated as
# quality failures, not acceptable structural nulls.
validation_rules = [
    (F.col("merchant_id").isNull(), "missing_merchant_id"),
    (F.col("campaign_id").isNull(), "missing_campaign_id"),
    (F.col("date").isNull(), "invalid_or_missing_date"),
    (F.col("spend").isNull(), "missing_spend"),
    (F.col("spend") < 0, "negative_spend"),
    (F.col("clicks") > F.col("impressions"), "clicks_exceed_impressions"),
]

validated_df = typed_df
reason_cols = []
for condition, reason in validation_rules:
    col_name = f"_fail_{reason}"
    validated_df = validated_df.withColumn(col_name, F.when(condition, F.lit(reason)))
    reason_cols.append(col_name)

validated_df = validated_df.withColumn(
    "_rejection_reason",
    F.array_join(F.array_compact(F.array(*[F.col(c) for c in reason_cols])), ",")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Split clean vs. rejected

# COMMAND ----------

clean_df = validated_df.filter(F.col("_rejection_reason") == "").drop(
    "_rejection_reason", *reason_cols
)

rejected_df = (
    validated_df.filter(F.col("_rejection_reason") != "")
    .select(*typed_df.columns, "_rejection_reason")
    .withColumn("_quarantined_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicate — window function, not dropDuplicates()
# MAGIC
# MAGIC Business key: `merchant_id + campaign_id + date`. When duplicates exist
# MAGIC (e.g. a late-arriving reprocessed batch), we keep the row with the latest
# MAGIC `ingestion_ts`. This is explicit and auditable — you can log how many
# MAGIC duplicates were dropped and which ingestion batch won.

# COMMAND ----------

dedup_window = Window.partitionBy("merchant_id", "campaign_id", "date").orderBy(
    F.col("ingestion_ts").desc()
)

deduped_df = (
    clean_df
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

duplicates_dropped = clean_df.count() - deduped_df.count()
print(f"Duplicates dropped: {duplicates_dropped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Enforce final schema and write Silver (strict, no mergeSchema)
# MAGIC
# MAGIC NOTE: the "classic" way to force a DataFrame into an exact StructType is
# MAGIC `spark.createDataFrame(df.rdd, schema=...)` — but `.rdd` requires direct
# MAGIC JVM/RDD access, which serverless (Spark Connect) doesn't expose, same
# MAGIC issue we hit on Day 1. `DataFrame.to(schema)` does the same job — casts
# MAGIC columns, fails loudly on incompatible nullability — through the ordinary
# MAGIC DataFrame API, so it works on serverless.
# MAGIC
# MAGIC **Nullability gotcha:** Spark tracks nullable as *schema metadata*, not
# MAGIC something inferred from your actual data. We already filtered out every
# MAGIC null in `merchant_id`/`campaign_id`/`market`/`date`/`ingestion_ts` back in
# MAGIC step 3 — but `filter()` doesn't flip the nullable flag, so the DataFrame's
# MAGIC schema still says `nullable=True` for those columns even though no nulls
# MAGIC actually remain. `.to(silver_schema)` checks that metadata and fails on
# MAGIC the mismatch. The fix below doesn't change any real values (the fallback
# MAGIC literal is never actually used, since the data's already clean) — it just
# MAGIC tells Spark's schema tracker "this can't be null," which flips the flag.

# COMMAND ----------

# MAGIC %md
# MAGIC **Extra gotcha on `date` specifically:** `F.lit("1970-01-01").cast(DateType())`
# MAGIC still won't work here — a `.cast()` expression is marked nullable by
# MAGIC Catalyst *regardless* of what's inside it, because casts can in general
# MAGIC fail (e.g. a malformed date string), so Spark conservatively keeps
# MAGIC `nullable=True` on any cast's output. The fix is to skip the cast
# MAGIC entirely and hand Spark a native Python `date` object as the literal —
# MAGIC that gets typed directly as `DateType` with no cast involved, so it's
# MAGIC correctly seen as non-nullable.

# COMMAND ----------

non_nullable_string_cols = ["merchant_id", "campaign_id", "market"]
for c in non_nullable_string_cols:
    deduped_df = deduped_df.withColumn(c, F.coalesce(F.col(c), F.lit("")))

deduped_df = deduped_df.withColumn(
    "date", F.coalesce(F.col("date"), F.lit(pydate(1970, 1, 1)))
)
deduped_df = deduped_df.withColumn(
    "ingestion_ts", F.coalesce(F.col("ingestion_ts"), F.current_timestamp())
)

final_df = deduped_df.to(silver_schema)

(
    final_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "false")   # deliberate: Silver schema drift should fail, not merge
    .partitionBy("market", "date")
    .saveAsTable(SILVER_TABLE)
)

(
    rejected_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")    # rejection reasons/shapes can evolve; quarantine is lenient
    .saveAsTable(REJECTED_TABLE)
)

print(f"Silver rows written: {final_df.count()}")
print(f"Rejected rows written: {rejected_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sanity checks

# COMMAND ----------

spark.table(SILVER_TABLE).limit(10).show(truncate=False)
spark.table(REJECTED_TABLE).groupBy("_rejection_reason").count().show(truncate=False)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview prep notes (read after running)
# MAGIC
# MAGIC - **"Why window function over dropDuplicates()?"** — `dropDuplicates()` picks an
# MAGIC   arbitrary surviving row when duplicates aren't byte-identical. The window
# MAGIC   approach makes "latest wins" an explicit, testable rule.
# MAGIC - **"Why quarantine instead of drop?"** — silent drops make pipelines
# MAGIC   untrustworthy: a stakeholder sees a 5% revenue dip and you can't tell them
# MAGIC   if it's real or a swallowed data quality issue. Quarantine tables make
# MAGIC   data loss observable and (with an alert on row count) actionable.
# MAGIC - **"Why is mergeSchema=false here but was true/permissive in Bronze?"** —
# MAGIC   this is the Bronze/Silver contract boundary. Bronze absorbs whatever
# MAGIC   arrives; Silver is where you promise downstream consumers a stable shape.
# MAGIC   An upstream schema change should surface here, loudly, in CI or in a job
# MAGIC   failure — not three layers downstream in a broken dashboard.