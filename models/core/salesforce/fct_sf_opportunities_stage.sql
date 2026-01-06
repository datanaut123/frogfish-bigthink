with
    oppor as (
        select
            opportunity_id,
            opportunity_name,
            company,
            owner_id,
            opportunity_amount,
            account_id,
            lead_source,
            opportunity_stage,
            contact_id,
            opportunity_created_date,
            close_date,
            funded_amount,
            commission,
            utm_campaign,
            utm_source,
            utm_medium,
            utm_content,
            utm_term,
            iso_name

        from {{ ref("stg_sf_opportunity") }}
    ),

    oppor_stages as (
        select opportunity_stage, opportunity_stage_change_date, opportunity_id

        from {{ ref("stg_sf_opportunity_stages_history") }}
    )

select
    coalesce(opp.opportunity_id, oppst.opportunity_id) as opportunity_id,
    opportunity_name,
    company,
    owner_id,
    opportunity_amount,
    account_id,
    lead_source,
    coalesce(oppst.opportunity_stage, opp.opportunity_stage) as opportunity_stage,
    lead_created_date,
    opportunity_stage_change_date,
    contact_id,
    opportunity_created_date,
    close_date,
    funded_amount,
    commission,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    utm_term,
    iso_name

from oppor as opp
left join oppor_stages as oppst on opp.opportunity_id = oppst.opportunity_id
left join
    (
        select distinct lead_id, lead_created_date, converted_opportunity_id
        from {{ ref("stg_sf_leads") }}
    ) as leads
    on opp.opportunity_id = leads.converted_opportunity_id
