SELECT DISTINCT
    converted_opportunity_id as opportunity_id,
    hot_lead_date AS date,
    'Hot Lead' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    funded_amount,
    commission,
    1 as hot_lead,
    0 as open_not_contacted,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as closed_converted
FROM {{ ref('fct_sf_leads_funds') }}
WHERE hot_lead_date is not null

UNION ALL

SELECT DISTINCT
    converted_opportunity_id as opportunity_id,
    open_not_contacted_date AS date,
    'Open Not Contacted' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    funded_amount,
    commission,
    0 as hot_lead,
    1 as open_not_contacted,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as closed_converted
FROM {{ ref('fct_sf_leads_funds') }}
WHERE open_not_contacted_date is not null

UNION ALL

SELECT DISTINCT
    converted_opportunity_id as opportunity_id,
    working_contacted_date AS date,
    'Working Contacted' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    funded_amount,
    commission,
    0 as hot_lead,
    0 as open_not_contacted,
    1 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    0 as closed_converted
FROM {{ ref('fct_sf_leads_funds') }}
WHERE working_contacted_date is not null

UNION ALL

SELECT DISTINCT
    converted_opportunity_id as opportunity_id,
    working_application_out_date AS date,
    'Working Application Out' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    funded_amount,
    commission,
    0 as hot_lead,
    0 as open_not_contacted,
    0 as working_contacted,
    1 as working_application_out,
    0 as closed_not_converted,
    0 as closed_converted
FROM {{ ref('fct_sf_leads_funds') }}
WHERE working_application_out_date is not null

UNION ALL

SELECT DISTINCT
    converted_opportunity_id as opportunity_id,
    closed_not_converted_date AS date,
    'Closed Not Converted' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    funded_amount,
    commission,
    0 as hot_lead,
    0 as open_not_contacted,
    0 as working_contacted,
    0 as working_application_out,
    1 as closed_not_converted,
    0 as closed_converted
FROM {{ ref('fct_sf_leads_funds') }}
WHERE closed_not_converted_date is not null

UNION ALL

SELECT DISTINCT
    converted_opportunity_id as opportunity_id,
    closed_converted_date AS date,
    'Closed Converted' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    funded_amount,
    commission,
    0 as hot_lead,
    0 as open_not_contacted,
    0 as working_contacted,
    0 as working_application_out,
    0 as closed_not_converted,
    1 as closed_converted
FROM {{ ref('fct_sf_leads_funds') }}
WHERE closed_converted_date is not null

