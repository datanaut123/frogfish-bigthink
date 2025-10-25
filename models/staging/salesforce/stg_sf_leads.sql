with leads as (
    select * from {{ source("Salesforce", "Lead") }}
)

select
    id as lead_id,
    name,
    email,
    company,
    leadsource as lead_source,
    date(createddate) as lead_created_date,
    -- mql_date__c does not exist in schema
    -- Most_Recent_MQL_Date__c does not exist in schema
    pi__utm_medium__c as utm_medium,
    pi__utm_source__c as utm_source,
    pi__utm_content__c as utm_content,
    pi__utm_campaign__c as utm_campaign,
    pi__utm_term__c as utm_term,
    -- lead_source_detail__c does not exist in schema
    csbs__Status_Detail__c as lead_status_detail,
    status as lead_status,
    convertedaccountid as converted_account_id,
    convertedcontactid as converted_contact_id

from leads