-- SCD Type 1 today, Type-2-shaped for tomorrow (effective_date included, no
-- history logic yet) — same design decision as Day 3's PySpark version.
-- materialized as 'table' (full rebuild, idempotent overwrite) via dbt_project.yml.

select
    row_number() over (order by merchant_id) as merchant_key,
    merchant_id,
    current_date() as effective_date
from (
    select distinct merchant_id
    from {{ source('silver', 'merchant_campaign_events') }}
)
