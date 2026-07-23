select
    date_start as date,
    safe_cast(campaign_id as int64) as campaign_id,
    campaign_name,
    sum(spend) as spend,
    sum(clicks) as clicks,
    sum(impressions) as impressions,
    'Meta' as platform,
    safe_cast(account_id as int64) as account_id,
    account_name,
    0 as conversions,
    'Heloc' as product

from {{ source("Meta_Ads_Heloc", "ads_insights") }}
group by date_start, campaign_id, campaign_name, account_id, account_name
