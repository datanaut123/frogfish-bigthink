SELECT DISTINCT
    converted_opportunity_id as opportunity_id,
    lead_created_date AS date,
    'Lead' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    1 AS leads,
    0 AS opportunity,
    0 AS prospecting,
    0 AS application_info,
    0 AS underwriting,
    0 AS approval_contract,
    0 AS closed_won,
    0 AS closed_lost
FROM {{ ref('fct_sf_leads') }}

UNION ALL

SELECT DISTINCT
    converted_opportunity_id as opportunity_id,
    converted_date AS date,
    'Converted to Opportunity' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    0 AS leads,
    1 AS opportunity,
    0 AS prospecting,
    0 AS application_info,
    0 AS underwriting,
    0 AS approval_contract,
    0 AS closed_won,
    0 AS closed_lost
FROM {{ ref('fct_sf_leads') }}
WHERE converted_date IS NOT NULL

UNION ALL

SELECT DISTINCT
    opportunity_id,
    Prospecting AS date,
    'Prospecting' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    0 AS leads,
    0 AS opportunity,
    1 AS prospecting,  
    0 AS application_info,
    0 AS underwriting,
    0 AS approval_contract,
    0 AS closed_won,
    0 AS closed_lost
FROM {{ ref('fct_sf_opportunities_stage') }}
WHERE Prospecting IS NOT NULL

UNION ALL

SELECT DISTINCT
    opportunity_id,
    Application_info AS date,
    'Application' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    0 AS leads,
    0 AS opportunity,
    0 AS prospecting,
    1 AS application_info,  
    0 AS underwriting,
    0 AS approval_contract,
    0 AS closed_won,
    0 AS closed_lost
FROM {{ ref('fct_sf_opportunities_stage') }}
WHERE Application_info IS NOT NULL

UNION ALL

SELECT DISTINCT
    opportunity_id,
    Underwriting AS date,
    'Underwriting' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    0 AS leads,
    0 AS opportunity,
    0 AS prospecting,
    0 AS application_info,
    1 AS underwriting, 
    0 AS approval_contract,
    0 AS closed_won,
    0 AS closed_lost
FROM {{ ref('fct_sf_opportunities_stage') }}
WHERE Underwriting IS NOT NULL

UNION ALL

SELECT DISTINCT
    opportunity_id,
    Approval AS date,
    'Approval Contract' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    0 AS leads,
    0 AS opportunity,
    0 AS prospecting,
    0 AS application_info,
    0 AS underwriting,
    1 AS approval_contract, 
    0 AS closed_won,
    0 AS closed_lost
FROM {{ ref('fct_sf_opportunities_stage') }}
WHERE Approval IS NOT NULL

UNION ALL

SELECT DISTINCT
    opportunity_id,
    Funded AS date,
    'Funded' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    0 AS leads,
    0 AS opportunity,
    0 AS prospecting,
    0 AS application_info,
    0 AS underwriting,
    0 AS approval_contract,
    1 AS closed_won,  
    0 AS closed_lost
FROM {{ ref('fct_sf_opportunities_stage') }}
WHERE Funded IS NOT NULL

UNION ALL

SELECT DISTINCT
    opportunity_id,
    Closed AS date,
    'Closed' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    0 AS leads,
    0 AS opportunity,
    0 AS prospecting,
    0 AS application_info,
    0 AS underwriting,
    0 AS approval_contract,
    0 AS closed_won,
    1 AS closed_lost  
FROM {{ ref('fct_sf_opportunities_stage') }}
WHERE Closed IS NOT NULL