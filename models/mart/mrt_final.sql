
select
    opportunity_id,
    date,
    stage,
    MAX(utm_term) AS utm_term, 
    MAX(utm_content) AS utm_content,
    MAX(utm_medium) AS utm_medium,
    MAX(utm_source) AS utm_source,
    MAX(utm_campaign) AS utm_campaign,
    sum(leads) as leads,
    sum(opportunity) as opportunity,
    sum(prospecting) as prospecting,
    sum(application_info) as application_info,
    sum(underwriting) as underwriting,
    sum(approval_contract) as approval_contract,
    sum(closed_won) as closed_won,
    sum(closed_lost) as closed_lost
from {{ ref("fct_sf_opportunities") }}
group by
    opportunity_id,
    date,
    stage
