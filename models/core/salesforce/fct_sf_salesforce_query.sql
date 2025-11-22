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
        select distinct
            lead_id,
            case when lead_stage = '3rd Attempt' then 1 else 0 end as is_3rd_attempt,
            case
                when lead_stage = 'Closed - Converted' then 1 else 0
            end as is_closed_converted,
            case
                when lead_stage = 'Closed - Not Converted' then 1 else 0
            end as is_closed_not_converted,
            case when lead_stage = 'FBIG Startup' then 1 else 0 end as is_fbig_startup,
            case when lead_stage = 'Hot Lead' then 1 else 0 end as is_hot_lead,
            case
                when lead_stage = 'Open - Not Contacted' then 1 else 0
            end as is_open_not_contacted,
            case
                when lead_stage = 'Working - Application Out' then 1 else 0
            end as is_working_application_out,
            case
                when lead_stage = 'Working - Contacted' then 1 else 0
            end as is_working_contacted

        from {{ ref('stg_sf_lead_history') }}

    ),

    opportunities as (
        select distinct
            opportunity_id,
            opportunity_created_date,
            case
                when opportunity_stage = 'Application In' then 1 else 0
            end as is_application_in,
            case
                when opportunity_stage = 'Application Missing Info' then 1 else 0
            end as is_application_missing_info,
            case when opportunity_stage = 'Approved' then 1 else 0 end as is_approved,
            case when opportunity_stage = 'Closed' then 1 else 0 end as is_closed,
            case
                when opportunity_stage = 'Contracts In' then 1 else 0
            end as is_contracts_in,
            case
                when opportunity_stage = 'Contracts Out' then 1 else 0
            end as is_contracts_out,
            case when opportunity_stage = 'Declined' then 1 else 0 end as is_declined,
            case when opportunity_stage = 'Funded' then 1 else 0 end as is_funded,
            case
                when opportunity_stage = 'House Pre-Approval' then 1 else 0
            end as is_house_pre_approval,
            case
                when opportunity_stage = 'In Underwriting' then 1 else 0
            end as is_in_underwriting,
            case when opportunity_stage = 'Pitched' then 1 else 0 end as is_pitched,
            case
                when opportunity_stage = 'Ready to Order Contracts' then 1 else 0
            end as is_ready_to_order_contracts,
            case when opportunity_stage = 'Referred' then 1 else 0 end as is_referred,
            case
                when opportunity_stage = 'Renewal Prospecting' then 1 else 0
            end as is_renewal_prospecting,
            case
                when opportunity_stage = 'Sent to Operations' then 1 else 0
            end as is_sent_to_operations,
            case when opportunity_stage = 'Submitted' then 1 else 0 end as is_submitted,
            case
                when opportunity_stage = 'Underwriting' then 1 else 0
            end as is_underwriting

        from {{ ref("fct_sf_opportunities_stage") }}
    )

select
    le.lead_id,
    coalesce(le.converted_opportunity_id, opp.opportunity_id) as opportunity_id,
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
    annual_revenue,
    company_website,
    lead_created_date,
    utm_term,
    utm_medium,
    utm_source,
    utm_content,
    utm_campaign,
    iso_name,
    is_3rd_attempt,
    is_closed_converted,
    is_closed_not_converted,
    is_fbig_startup,
    is_hot_lead,
    is_open_not_contacted,
    is_working_application_out,
    is_working_contacted,
    is_application_in,
    is_application_missing_info,
    is_approved,
    is_closed,
    is_contracts_in,
    is_contracts_out,
    is_declined,
    is_funded,
    is_house_pre_approval,
    is_in_underwriting,
    is_pitched,
    is_ready_to_order_contracts,
    is_referred,
    is_renewal_prospecting,
    is_sent_to_operations,
    is_submitted,
    is_underwriting

from leads as le
left join lead_history as lh on le.lead_id = lh.lead_id
full join opportunities as opp on le.converted_opportunity_id = opp.opportunity_id
