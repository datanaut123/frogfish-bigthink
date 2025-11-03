with leads as (
select
    opportunity_id,
    date,
    stage,
    MAX(utm_term) AS utm_term, 
    MAX(utm_content) AS utm_content,
    MAX(utm_medium) AS utm_medium,
    MAX(utm_source) AS utm_source,
    MAX(utm_campaign) AS utm_campaign,
    funded_amount,
    commission,
    sum(hot_lead) as hot_lead,
    sum(open_not_contacted) as open_not_contacted,
    sum(working_contacted) as working_contacted,
    sum(working_application_out) as working_application_out,
    sum(closed_not_converted) as closed_not_converted,
    sum(closed_converted) as closed_converted

from {{ ref("fct_sf_opportunities") }}
group by
    opportunity_id,
    date,
    stage,
    funded_amount,
    commission
),

    ads as (
        select
            date,
            campaign_id,
            campaign_name,
            platform,
            sum(spend) as spend,
            sum(clicks) as clicks,
            sum(impressions) as impressions
        from {{ ref("mrt_ads") }}
        group by date, campaign_id, campaign_name, platform
    ),

    data_join as (
        select
            coalesce(a.date, l.date) as date,
            a.date as a_date,
            a.campaign_id as a_campaign_id,
            utm_term,
            utm_medium,
            utm_source,
            utm_content,
            coalesce(a.campaign_name, l.utm_campaign) as campaign_name,
            {# coalesce(a.platform, l.platform) as platform, #}
            a.platform,
            funded_amount,
            commission,
            hot_lead,
            open_not_contacted,
            working_contacted,
            working_application_out,
            closed_not_converted,
            closed_converted,
            spend,
            clicks,
            impressions

        from ads as a
        full join leads as l on a.date = l.date and a.campaign_name = l.utm_campaign
    ),
    deduplication as (
        select
            *,
            row_number() over (
                partition by a_date, a_campaign_id order by a_date asc

            ) as rn
        from data_join
    )
select
    * except (spend, impressions, clicks, a_date, a_campaign_id),
    case when rn = 1 then spend else 0 end as spend,
    case when rn = 1 then impressions else 0 end as impressions,
    case when rn = 1 then clicks else 0 end as clicks

from deduplication