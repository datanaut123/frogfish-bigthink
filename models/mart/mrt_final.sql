with leads as (
select
    Date(date) as date,
    case when utm_medium like '%cpc%' then 'Google' else 'Other' end as platform,
    case when utm_medium like '%cpc%'then 'Paid' else 'Other' end as channel,
    sum(funded_amount) as funded_amount,
    sum(commission) as commission,
    sum(hot_lead) as hot_lead,
    sum(working_contacted) as working_contacted,
    sum(working_application_out) as working_application_out,
    sum(application_in) as application_in,
    sum(approved) as approved,
    sum(declined) as declined,
    sum(contract_in) as contract_in,
    sum(contract_out) as contract_out,
    sum(funded) as funded

from {{ ref("fct_sf_opportunities") }}

group by
    date,
    utm_medium
),

    ads as (
        select
            date(date) as date,
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
            working_contacted,
            working_application_out,
            application_in,
            approved,
            declined,
            contract_in,
            contract_out,
            funded,
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
    * except (spend, impressions, clicks, a_date, a_platform, a_channel),
    case when rn = 1 then spend else 0 end as spend,
    case when rn = 1 then impressions else 0 end as impressions,
    case when rn = 1 then clicks else 0 end as clicks

from deduplication