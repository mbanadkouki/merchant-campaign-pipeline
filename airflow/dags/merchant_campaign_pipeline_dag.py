"""
Day 6 — merchant_campaign_pipeline DAG

Orchestrates the full pipeline built across Days 1-5:
  Bronze (PySpark) -> Silver (PySpark) -> Gold (PySpark) -> Gold (dbt) -> Postgres

Design notes (interview talking points):
1. Each task is a thin BashOperator wrapping an existing, already-tested script —
   Airflow's job here is ordering and failure handling, not reimplementing logic.
2. Strict linear dependency chain: if any stage fails, everything downstream is
   skipped rather than running against stale/partial data. This is the automated
   version of the discipline we kept manually across Days 1-5 (never running
   Day 3 before confirming Day 2 succeeded).
3. Gold is built twice on purpose (PySpark in Day 3, dbt in Day 4) — both are
   included here to preserve that history, but in a real team this DAG would
   need to pick ONE owner for the gold schema, not run both against the same
   tables. Flagged here rather than silently resolved.
4. retries=1 on each task: a transient network blip (e.g. a Neon connection
   hiccup) shouldn't fail the whole pipeline outright, but a real, repeated
   failure still surfaces after one retry rather than retrying forever.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# All project files are mounted into the container at this path (see
# docker-compose.yaml volumes: - ../:/opt/airflow/project)
PROJECT_DIR = "/opt/airflow/project"

# dbt needs its own working directory and profiles dir, same as running it
# locally — profiles.yml is expected at ~/.dbt/profiles.yml inside the
# container too, so that also needs to be mounted (see setup notes).
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt/merchant_campaign_dbt"

default_args = {
    "owner": "mohsen",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="merchant_campaign_pipeline",
    description="Bronze -> Silver -> Gold (PySpark + dbt) -> Postgres serving layer",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,   # manual trigger for now; a real cron schedule comes once this is proven stable
    catchup=False,   # don't backfill runs for every day since start_date — we only care about "now"
    tags=["merchant-campaign-pipeline"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"cd {PROJECT_DIR} && python day01_bronze_ingestion.py",
    )

    silver = BashOperator(
        task_id="silver_transformation",
        bash_command=f"cd {PROJECT_DIR} && python day02_silver_transformation.py",
    )

    gold_pyspark = BashOperator(
        task_id="gold_layer_pyspark",
        bash_command=f"cd {PROJECT_DIR} && python day03_gold_layer.py",
    )

    gold_dbt = BashOperator(
        task_id="gold_layer_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run && dbt test",
    )

    postgres_load = BashOperator(
        task_id="postgres_serving_load",
        bash_command=f"cd {PROJECT_DIR} && python day05_postgres_load.py",
    )

    # Linear chain: each stage strictly depends on the previous one succeeding.
    bronze >> silver >> gold_pyspark >> gold_dbt >> postgres_load
