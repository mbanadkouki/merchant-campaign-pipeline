select
    row_number() over (order by market) as market_key,
    market,
    current_date() as effective_date
from (
    select distinct market
    from {{ source('silver', 'merchant_campaign_events') }}
)
