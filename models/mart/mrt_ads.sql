select
    date, campaign_id, campaign_name, spend, clicks, impressions, 'Google' as platform

from {{ ref("stg_ga_campaigns") }}