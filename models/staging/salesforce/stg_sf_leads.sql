with leads as (
    select * from {{ source("Salesforce", "Lead") }}
)


select
    Id as lead_id,
    Name as name,
    Email as email,
    Company as company,
    LeadSource as lead_source,
    Status as lead_status,
    csbs__Status_Detail__c as lead_status_detail,
    IsConverted as is_converted,
    City as city,
    Country as country,
    Industry as industry,
    OwnerId as owner_id,
    ConvertedAccountId as converted_account_id,
    ConvertedContactId as converted_contact_id,
    ConvertedOpportunityId as converted_opportunity_id,
    AnnualRevenue as annual_revenue,
    Website as company_website,
    DATE(CreatedDate) as lead_created_date,
    DATE(converteddate) AS lead_converted_date,
    DATE(last_status_change__c) AS last_status_change_date,
    csbs__UTM_Term__c as utm_term,
    csbs__UTM_Medium__c as utm_medium,
    csbs__UTM_Source__c as utm_source,
    csbs__UTM_Content__c as utm_content,
    csbs__UTM_Campaign__c as utm_campaign,
    ISO_Name__c as iso_name,
    GCLID__c as gclid

from leads
