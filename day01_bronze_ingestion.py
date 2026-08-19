# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Day 1 — Bronze Layer
# MAGIC Synthetic merchant campaign data generation + raw ingestion to Delta.
# MAGIC
# MAGIC **Concept practiced:** cluster/driver/executor model, lazy evaluation,
# MAGIC partitioning — you'll see all three in action below.

# COMMAND ----------

import random
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import spark_partition_id, countDistinct
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SparkSession and target schema
# MAGIC The driver's entry point, plus making sure the Unity Catalog schema
# MAGIC (`workspace.bronze`) exists before we try to write to it.

# COMMAND ----------

spark = SparkSession.builder.appName("merchant-campaign-bronze").getOrCreate()

# Databricks Free Edition's default catalog is called "workspace".
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explicit source schema
# MAGIC No lazy schema inference in production pipelines — this is the data
# MAGIC contract for what upstream is expected to send us.

# COMMAND ----------

schema = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("market", StringType(), False),
    StructField("date", DateType(), False),
    StructField("spend", DoubleType(), True),
    StructField("impressions", IntegerType(), True),
    StructField("clicks", IntegerType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate synthetic campaign event data

# COMMAND ----------

MARKETS = ["DE", "NL", "PL", "IT", "FR"]
MERCHANTS = [f"m_{i:04d}" for i in range(1, 51)]     # 50 merchants
CAMPAIGNS = [f"c_{i:04d}" for i in range(1, 201)]    # 200 campaigns

def random_date(start: date, end: date) -> date:
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))

def generate_rows(n: int):
    start, end = date(2026, 1, 1), date(2026, 6, 30)
    for _ in range(n):
        impressions = random.randint(100, 50000)
        # click-through rate roughly 0.5% - 4%, with some noise
        clicks = int(impressions * random.uniform(0.005, 0.04))
        spend = round(clicks * random.uniform(0.15, 1.2), 2)  # cost per click model
        yield (
            random.choice(MERCHANTS),
            random.choice(CAMPAIGNS),
            random.choice(MARKETS),
            random_date(start, end),
            spend,
            impressions,
            clicks,
        )

# Nothing has actually executed yet up to this point beyond Python-side generation.
rows = list(generate_rows(20000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build the DataFrame + stamp ingestion time
# MAGIC Still lazy — no Spark job has run yet. `ingestion_ts` is added here
# MAGIC (not in the source schema above) because it's metadata *we* generate
# MAGIC at the lake boundary, not something upstream sends us. This is what
# MAGIC Silver's "keep latest by ingestion_ts" dedup logic will key on.

# COMMAND ----------

df = spark.createDataFrame(rows, schema=schema)
df = df.withColumn("ingestion_ts", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Check partition count (still lazy)
# MAGIC `df.rdd.getNumPartitions()` is **not available on serverless compute** —
# MAGIC serverless uses Spark Connect, which doesn't expose direct RDD/JVM access.
# MAGIC `spark_partition_id()` gets the same answer through the ordinary
# MAGIC DataFrame/SQL API instead.

# COMMAND ----------

initial_partitions = (
    df.select(spark_partition_id().alias("pid"))
    .agg(countDistinct("pid"))
    .collect()[0][0]
)
print(f"Initial partitions: {initial_partitions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Repartition by market
# MAGIC Partitioning by a low-cardinality, frequently-filtered column enables
# MAGIC partition pruning on read, and keeps per-partition write file sizes
# MAGIC reasonable.

# COMMAND ----------

df = df.repartition("market")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Bronze layer
# MAGIC This `.write` call is the action that finally triggers execution
# MAGIC across the cluster — everything above this point was just building
# MAGIC a lazy plan.

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("market")
    .saveAsTable("workspace.bronze.merchant_campaign_events")
)

print("Bronze ingestion complete.")
df.groupBy("market").count().show()