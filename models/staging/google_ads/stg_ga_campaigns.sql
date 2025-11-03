with google_ads_campaigns as (
    select * from {{ source("Google_Ads", "campaign") }}
)

select
    campaign_id,
    campaign_name,
    segments_date as date,
    metrics_clicks as clicks,
    metrics_cost_micros / 1000000.0 as spend,
    metrics_impressions as impressions,
    metrics_conversions as conversions,
    cast(regexp_extract(campaign_resource_name, r'customers/(\d+)/') as int64) as account_id

from google_ads_campaigns