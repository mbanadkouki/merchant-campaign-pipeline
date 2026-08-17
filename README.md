# merchant-campaign-pipeline
End-to-end merchant campaign analytics pipeline (Databricks, Delta Lake, dbt, Airflow, Postgres, FastAPI) 

A hands-on, end-to-end data engineering project simulating how an
advertising platform turns raw merchant campaign events into reliable,
decision-ready metrics — the same core problem faced by teams building
campaign decision systems for e-commerce marketplaces.

**Business goal:** ingest raw campaign event data (impressions, clicks,
spend) across multiple markets and merchants, clean and model it through
a Medallion architecture (Bronze → Silver → Gold), serve trustworthy
daily performance metrics (CTR, spend efficiency) via a fast query layer
and a REST API, and keep the whole pipeline observable and reliable.

**Stack:** Databricks / Spark, Delta Lake, dbt, Apache Airflow,
PostgreSQL, FastAPI.

