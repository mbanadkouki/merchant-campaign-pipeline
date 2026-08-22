-- Note: dims below are pulled in via dbt's ref() function (not source()) --
-- that's what tells dbt these models depend on the dimensions and must
-- run after them. dbt builds this ordering into the DAG automatically
-- from these ref() calls.

with events as (
    select * from {{ source('silver', 'merchant_campaign_events') }}
),

merchants as (
    select * from {{ ref('dim_merchant') }}
),

markets as (
    select * from {{ ref('dim_market') }}
)

select
    m.merchant_key,
    mk.market_key,
    e.campaign_id,
    e.date,
    e.spend,
    e.impressions,
    e.clicks,
    -- null-safe: explicit 0.0 on zero-denominator instead of a silent null,
    -- same reasoning as the F.when() version in Day 3's PySpark model
    case when e.impressions > 0 then e.clicks / e.impressions else 0.0 end as ctr,
    case when e.clicks > 0 then e.spend / e.clicks else 0.0 end as cost_per_click
from events e
inner join merchants m on e.merchant_id = m.merchant_id
inner join markets mk on e.market = mk.market