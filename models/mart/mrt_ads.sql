with accounts as (select
    date, campaign_id, campaign_name, spend, clicks, impressions, 'Google' as platform, a.account_id, b.account_name

from {{ ref("stg_ga_campaigns") }} as a 
left join {{ ref("stg_ga_accounts") }} as b on a.account_id = b.account_id
)

select * from accounts 
where account_id = 8937368243