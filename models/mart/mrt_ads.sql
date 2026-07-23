with
    accounts as (
        select
            date,
            campaign_id,
            campaign_name,
            spend,
            clicks,
            impressions,
            'Google' as platform,
            a.account_id,
            b.account_name,
            conversions,
            case
                when lower(campaign_name) like '%heloc%' then 'Heloc' else 'Others'
            end as product

        from {{ ref("stg_ga_campaigns") }} as a
        left join {{ ref("stg_ga_accounts") }} as b on a.account_id = b.account_id
    )

select *
from accounts
where account_id = 8937368243

union all 
select * from {{ref("stg_ma_ads_insights")}}
