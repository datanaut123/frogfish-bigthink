{# select distinct
    lead_id,
    name,
    email,
    company,
    lead_source,
    lead_status,
    lead_status_detail,
    is_converted,
    city,
    country,
    industry,
    owner_id,
    converted_account_id,
    converted_contact_id,
    annual_revenue,
    company_website,
    lead_created_date,
    utm_medium,
    utm_source,
    utm_content,
    utm_campaign,
    utm_term

from {{ref('stg_sf_leads')}} #}

SELECT DISTINCT
    lead_id,
    lead_created_date AS date,
    'Lead' AS stage,
    utm_campaign,
    utm_source,
    utm_medium,
    utm_term,
    utm_content,
    CASE
        WHEN lead_source IN ('BTC Website', 'Website', 'Web Form') THEN 'BTC Website'
        WHEN lead_source IN ('Affiliate - Meridian 102', 'Affiliate - Meridian') THEN 'Affiliate - Meridian'
        WHEN lead_source = 'Affiliate - Fundwise' THEN 'Affiliate - Fundwise'
        WHEN lead_source = 'Affiliate - Charlie Chang' THEN 'Affiliate - Charlie Chang'
        WHEN lead_source = 'Google Ads' THEN 'Google Ads'
        WHEN lead_source = 'TikTok Blue' THEN 'TikTok Ads'
        WHEN lead_source = 'Application Parser' THEN 'System / Integration'
        WHEN lead_source = 'Manual QA Test' THEN 'Internal / Test'
        WHEN lead_source IS NULL OR lead_source = 'null' THEN 'Unknown'
        ELSE lead_source
    END AS platform,
    0 AS opportunity_amount,
    0 AS closed_won_amount,
    1 AS lead,
    0 AS mqls,
    0 AS sals,
    0 AS sqls,
    0 AS sao,
    0 AS closed_won

FROM {{ ref('stg_sf_leads') }}

