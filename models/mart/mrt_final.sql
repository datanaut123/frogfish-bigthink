with leads as (
select
    date,
    stage,
    'Google' as platform,
    sum(funded_amount) as funded_amount,
    sum(commission) as commission,
    sum(hot_lead) as hot_lead,
    sum(open_not_contacted) as open_not_contacted,
    sum(working_contacted) as working_contacted,
    sum(working_application_out) as working_application_out,
    sum(closed_not_converted) as closed_not_converted,
    sum(closed_converted) as closed_converted

from {{ ref("fct_sf_opportunities") }}
where lead_source = 'Google Ads'
group by
    date,
    stage,
    utm_term, 
    utm_content,
    utm_medium,
    utm_source,
    utm_campaign
),

    ads as (
        select
            date,
            platform,
            sum(spend) as spend,
            sum(clicks) as clicks,
            sum(impressions) as impressions,

        from {{ ref("mrt_ads") }}
        group by date, campaign_id, campaign_name, platform
    ),

    data_join as (
        select
            coalesce(a.date, l.date) as date,
            a.date as a_date,
            coalesce(a.platform, l.platform) as platform,
            a.platform as a_platform,
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
        full join leads as l on a.date = l.date and a.platform = l.platform
    ),
    deduplication as (
        select
            *,
            row_number() over (
                partition by a_date, a_platform order by a_date asc

            ) as rn
        from data_join
    )
select
    * except (spend, impressions, clicks, a_date, a_platform),
    case when rn = 1 then spend else 0 end as spend,
    case when rn = 1 then impressions else 0 end as impressions,
    case when rn = 1 then clicks else 0 end as clicks

from deduplication