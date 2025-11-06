with leads as (
select
    date,
    case when lead_source = 'Google Ads' then 'Google' else lead_source end as platform,
    case when lead_source = 'Google Ads' then 'Paid' else 'Organic' end as channel,
    sum(funded_amount) as funded_amount,
    sum(commission) as commission,
    sum(hot_lead) as hot_lead,
    sum(open_not_contacted) as open_not_contacted,
    sum(working_contacted) as working_contacted,
    sum(working_application_out) as working_application_out,
    sum(closed_not_converted) as closed_not_converted,
    sum(closed_converted) as closed_converted

from {{ ref("fct_sf_opportunities") }}
group by
    date,
    lead_source
),

    ads as (
        select
            date,
            platform,
            'Paid' as channel,
            sum(spend) as spend,
            sum(clicks) as clicks,
            sum(impressions) as impressions,

        from {{ ref("mrt_ads") }}
        group by date,  platform
    ),

    data_join as (
        select
            coalesce(a.date, l.date) as date,
            a.date as a_date,
            coalesce(a.platform, l.platform) as platform,
            coalesce(a.channel, l.channel) as channel,
            a.platform as a_platform,
            a.channel as a_channel,
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
        full join leads as l on a.date = l.date and a.platform = l.platform and a.channel = l.channel
    ),
    deduplication as (
        select
            *,
            row_number() over (
                partition by a_date, a_platform, a_channel order by a_date asc

            ) as rn
        from data_join
    )
select
    * except (spend, impressions, clicks, a_date, a_platform),
    case when rn = 1 then spend else 0 end as spend,
    case when rn = 1 then impressions else 0 end as impressions,
    case when rn = 1 then clicks else 0 end as clicks

from deduplication