with
    leads as (
        select
            date(date) as date,
            case
                when iso_name = 'BTC Google' then 'Google' else 'Other'
            end as platform,
            case when iso_name = 'BTC Google' then 'Paid' else 'Other' end as channel,
            case
                when iso_name = 'BTC Google' then utm_source else 'Unknown'
            end as campaign_name,
            sum(funded_amount) as funded_amount,
            sum(commission) as commission,
            sum(hot_lead) as hot_lead,
            sum(working_contacted) as working_contacted,
            sum(working_application_out) as working_application_out,
            sum(closed_not_converted) as closed_not_converted,
            sum(application_in) as application_in,
            sum(approved) as approved,
            sum(declined) as declined,
            sum(contracts_in) as contract_in,
            sum(contracts_out) as contract_out,
            sum(funded) as funded

        from {{ ref("fct_sf_opportunities_unfreeze_stages") }}

        group by date, iso_name, utm_source
    ),

    ads as (
        select
            date(date) as date,
            platform,
            'Paid' as channel,
            campaign_name,
            case
                when campaign_name = 'FF - Search - Leads - Campaign #1'
                then 'search_leads'
                when campaign_name = 'FF - Search - Brand - Leads'
                then 'search_brand'
                else 'Other'
            end as join_campaign,
            sum(spend) as spend,
            sum(clicks) as clicks,
            sum(impressions) as impressions,

        from {{ ref("mrt_ads") }}
        group by date, platform, campaign_name
    ),

    data_join as (
        select
            coalesce(a.date, l.date) as date,
            a.date as a_date,
            coalesce(a.platform, l.platform) as platform,
            coalesce(a.channel, l.channel) as channel,
            coalesce(a.campaign_name, l.campaign_name) as campaign_name,
            a.campaign_name as a_campaign_name,
            a.platform as a_platform,
            a.channel as a_channel,
            funded_amount,
            commission,
            hot_lead,
            working_contacted,
            working_application_out,
            closed_not_converted,
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
        full join
            leads as l
            on (
                a.date = l.date
                and a.platform = l.platform
                and a.channel = l.channel
                and l.campaign_name = a.join_campaign
            )
    ),
    deduplication as (
        select
            *,
            row_number() over (
                partition by a_date, a_platform, a_channel, a_campaign_name order by a_date asc

            ) as rn
        from data_join
    )
select
    * except (spend, impressions, clicks, a_date, a_platform, a_channel, a_campaign_name),
    case when rn = 1 then spend else 0 end as spend,
    case when rn = 1 then impressions else 0 end as impressions,
    case when rn = 1 then clicks else 0 end as clicks

from deduplication
