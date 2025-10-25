with contacts as (
    select * from {{ source("Salesforce", "Contact") }}
)

select
    id as contact_id,
    name,
    email,
    ownerid as owner_id,
    accountid as account_id,
    date(createddate) as contact_created_at,
    pi__utm_term__c as utm_term,
    pi__utm_medium__c as utm_medium,
    pi__utm_source__c as utm_source,
    pi__utm_content__c as utm_content,
    pi__utm_campaign__c as utm_campaign,
    contactsource as contact_source

from contacts