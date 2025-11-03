with leads as (
    SELECT
    lead_id,
    name,
    email,
    company,
    platform,
    industry,
    lead_status,
    lead_status_detail,
    converted_opportunity_id,
    lead_created_date,
    lead_converted_date,
    last_status_change_date,
    utm_term,
    utm_medium,
    utm_source,
    utm_content,
    utm_campaign,
    is_converted,
    hot_lead_date,
    open_not_contacted_date,
    working_contacted_date,
    working_application_out_date,
    closed_not_converted_date,
    closed_converted_date
    from {{ref('fct_sf_leads')}}
),
opportunity as (
    SELECT 
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
        record_type_id,
        funded_amount,
        commission
    from {{ref('stg_sf_opportunity')}}
)

SELECT 
    l.*,
    o.opportunity_name,
    o.owner_id,
    o.opportunity_amount,
    o.account_id,
    o.lead_source,
    o.opportunity_stage,
    o.contact_id,
    o.opportunity_created_date,
    o.close_date,
    o.record_type_id,
    o.funded_amount,
    o.commission
FROM leads l
LEFT JOIN opportunity o 
    ON l.converted_opportunity_id = o.opportunity_id