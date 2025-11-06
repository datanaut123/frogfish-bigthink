select distinct
    converted_opportunity_id as opportunity_id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    lead_source,
    0 as funded_amount,
    0 as commission,
    1 as hot_lead,
    0 as open_not_contacted,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as closed_converted
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Hot Lead'

union all

select distinct
    converted_opportunity_id as opportunity_id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    lead_source,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    1 as open_not_contacted,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as closed_converted
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Open - Not Contacted'

union all

select distinct
    converted_opportunity_id as opportunity_id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    lead_source,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as open_not_contacted,
    1 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as closed_converted
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Working - Contacted'

union all

select distinct
    converted_opportunity_id as opportunity_id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    lead_source,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as open_not_contacted,
    0 as working_contacted,
    1 as working_application_out,
    0 as closed_not_converted,
    0 as closed_converted
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Working - Application Out'

union all

select distinct
    converted_opportunity_id as opportunity_id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    lead_source,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as open_not_contacted,
    0 as working_contacted,
    0 as working_application_out,
    1 as closed_not_converted,
    0 as closed_converted
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Closed - Not Converted'

union all

select distinct
    converted_opportunity_id as opportunity_id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    lead_source,
    funded_amount,
    commission,
    0 as hot_lead,
    0 as open_not_contacted,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    1 as closed_converted
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Closed - Converted'
