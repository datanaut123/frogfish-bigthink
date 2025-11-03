with osh as (
select distinct
    id,
    opportunity_amount,
    opportunity_close_date,
    opportunity_stage,
    prev_opportunity_amount,
    opportunity_stage_change_date,
    opportunity_id,
    CreatedById

from {{ref('stg_sf_opportunity_stages_history')}}
),

os as (
    select distinct
        stage_id,
        stage_status,
        {# opportunity_stage_change_date, #}
        CreatedById
    from {{ref('stg_sf_opportunity_stage')}}
),

o as (
    select distinct
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
),
leads as (
    select
    converted_opportunity_id,
    company,
    utm_term,
    utm_medium,
    utm_source,
    utm_content,
    utm_campaign
    from {{ref('stg_sf_leads')}}
),
stages AS (
SELECT
    opportunity_id,
    MIN(CASE WHEN opportunity_stage IN ('Pitched', 'Referred', 'Renewal Prospecting') 
        THEN opportunity_stage_change_date END) AS Prospecting,
    MIN(CASE WHEN opportunity_stage IN ('Application In', 'Application Missing Info') 
        THEN opportunity_stage_change_date END) AS Application_info,
    MIN(CASE WHEN opportunity_stage = 'Underwriting' 
        THEN opportunity_stage_change_date END) AS Underwriting,
    MIN(CASE WHEN opportunity_stage IN ('Approved', 'Sent to Operations', 'Ready to Order Contracts', 'Contracts Out', 'Contracts In') 
        THEN opportunity_stage_change_date END) AS Approval,
    MIN(CASE WHEN opportunity_stage = 'Funded' 
        THEN opportunity_stage_change_date END) AS Funded,
    MIN(CASE WHEN opportunity_stage IN ('Declined', 'Closed') 
        THEN opportunity_stage_change_date END) AS Closed
    FROM {{ ref('stg_sf_opportunity_stages_history') }}
    GROUP BY opportunity_id
)
select 
    osh.id,
    osh.opportunity_amount,
    osh.opportunity_close_date,
    osh.opportunity_stage,
    osh.prev_opportunity_amount,
    osh.opportunity_stage_change_date,
    osh.opportunity_id,
    {# os.stage_id,
    os.stage_status, #}
    o.opportunity_name,
    o.company,
    o.owner_id,
    o.account_id,
    o.lead_source,
    o.contact_id,
    o.opportunity_created_date,
    o.close_date,
    o.record_type_id,
    o.funded_amount,
    o.commission,
    l.utm_term,
    l.utm_medium,
    l.utm_source,
    l.utm_content,
    l.utm_campaign,
    s.* EXCEPT(opportunity_id)
FROM o
{# left join os on osh.CreatedById = os.CreatedById #}
left join osh on osh.opportunity_id = o.opportunity_id
left join leads as l on l.converted_opportunity_id = o.opportunity_id
left join stages as s on s.opportunity_id = o.opportunity_id
