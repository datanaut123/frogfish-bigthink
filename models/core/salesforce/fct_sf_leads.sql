with
    leads as (
        select
            lead_id,
            name,
            email,
            company,
            lead_source,
            lead_status,
            is_converted,
            city,
            country,
            industry,
            owner_id,
            converted_account_id,
            converted_contact_id,
            converted_opportunity_id,
            annual_revenue,
            company_website,
            lead_created_date,
            utm_term,
            utm_medium,
            utm_source,
            utm_content,
            utm_campaign,
            iso_name

        from {{ ref('stg_sf_leads') }}
    ),

    lead_history as (
        select
            lead_id,
            lead_stage,
            lead_stage_date

        from {{ ref('stg_sf_lead_history') }}

    ),

opportunities as (
    SELECT distinct 
        opportunity_id,
        opportunity_name,
        opportunity_amount,
        lead_source,
        opportunity_created_date,
        funded_amount,
        commission
    from {{ref('stg_sf_opportunity')}}
)

select
    coalesce(le.lead_id, lh.lead_id) as lead_id,
    opportunity_id,
    opportunity_name,
    opportunity_amount,
    opportunity_created_date,
    funded_amount,
    commission,
    lead_stage,
    lead_stage_date,
    name,
    email,
    company,
    coalesce(le.lead_source,op.lead_source) as lead_source,
    lead_status,
    is_converted,
    city,
    country,
    industry,
    owner_id,
    converted_account_id,
    converted_contact_id,
    converted_opportunity_id,
    annual_revenue,
    company_website,
    lead_created_date,
    utm_term,
    utm_medium,
    utm_source,
    utm_content,
    utm_campaign,
    iso_name

from leads as le
left join lead_history as lh on le.lead_id = lh.lead_id
left join opportunities as op on le.converted_opportunity_id = op.opportunity_id
