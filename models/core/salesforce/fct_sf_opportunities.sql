select distinct
    lead_id as id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    1 as hot_lead,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    0 as approved,
    0 as declined,
    0 as contract_in,
    0 as contract_out,
    0 as funded,
    1 as is_lead,
    0 as is_opportunity

from {{ ref('fct_sf_leads') }}
where lead_stage = 'Hot Lead'

union all

select distinct
    lead_id as id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    1 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    0 as approved,
    0 as declined,
    0 as contract_in,
    0 as contract_out,
    0 as funded,
    1 as is_lead,
    0 as is_opportunity
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Working - Contacted'

union all

select distinct
    lead_id as id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as working_contacted,
    1 as working_application_out,
    0 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    0 as approved,
    0 as declined,
    0 as contract_in,
    0 as contract_out,
    0 as funded,
    1 as is_lead,
    0 as is_opportunity
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Working - Application Out'

union all

select distinct
    lead_id as id,
    lead_stage_date as date,
    lead_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as working_contacted,
    0 as working_application_out,
    1 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    0 as approved,
    0 as declined,
    0 as contract_in,
    0 as contract_out,
    0 as funded,
    1 as is_lead,
    0 as is_opportunity
from {{ ref('fct_sf_leads') }}
where lead_stage = 'Closed - Not Converted'

union all

select distinct
    opportunity_id as id,
    opportunity_stage_change_date as date,
    opportunity_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    1 as application_in,
    0 as submitted_lender,
    0 as approved,
    0 as declined,
    0 as contract_in,
    0 as contract_out,
    0 as funded,
    0 as is_lead,
    1 as is_opportunity
from {{ ref('fct_sf_opportunities_stage') }}
where opportunity_stage = 'Application In'

union all

select distinct
    opportunity_id as id,
    opportunity_stage_change_date as date,
    opportunity_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    1 as approved,
    0 as declined,
    0 as contract_in,
    0 as contract_out,
    0 as funded,
    0 as is_lead,
    1 as is_opportunity
from {{ ref('fct_sf_opportunities_stage') }}
where opportunity_stage = 'Approved'

union all

select distinct
    opportunity_id as id,
    opportunity_stage_change_date as date,
    opportunity_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    0 as approved,
    1 as declined,
    0 as contract_in,
    0 as contract_out,
    0 as funded,
    0 as is_lead,
    1 as is_opportunity
from {{ ref('fct_sf_opportunities_stage') }}
where opportunity_stage = 'Declined'

union all

select distinct
    opportunity_id as id,
    opportunity_stage_change_date as date,
    opportunity_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    0 as approved,
    0 as declined,
    1 as contract_in,
    0 as contract_out,
    0 as funded,
    0 as is_lead,
    1 as is_opportunity
from {{ ref('fct_sf_opportunities_stage') }}
where opportunity_stage = 'Contracts In'

union all

select distinct
    opportunity_id as id,
    opportunity_stage_change_date as date,
    opportunity_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    0 as funded_amount,
    0 as commission,
    0 as hot_lead,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    0 as approved,
    0 as declined,
    0 as contract_in,
    1 as contract_out,
    0 as funded,
    0 as is_lead,
    1 as is_opportunity
from {{ ref('fct_sf_opportunities_stage') }}
where opportunity_stage = 'Contracts Out'

union all

select distinct
    opportunity_id as id,
    opportunity_stage_change_date as date,
    opportunity_stage as stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_content,
    lead_source,
    iso_name,
    funded_amount,
    commission,
    0 as hot_lead,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as application_in,
    0 as submitted_lender,
    0 as approved,
    0 as declined,
    0 as contract_in,
    0 as contract_out,
    1 as funded,
    0 as is_lead,
    1 as is_opportunity
from {{ ref('fct_sf_opportunities_stage') }}
where opportunity_stage = 'Funded'

