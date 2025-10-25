with opps as (select * from {{ source("Salesforce", "Opportunity") }})

select
    id as opportunity_id,
    name as opportunity_name,
    TRIM(SPLIT(name, '|')[OFFSET(0)]) AS company,
    amount as opportunity_amount,
    accountid as account_id,
    leadsource as lead_source,
    StageName as opportunity_stage,
    contactid as contact_id,
    date(createddate) as opportunity_created_date,
    {# date(closedate) as close_date, #}
    {# date(First_Close_Date__c) as first_close_date, #}
    {# date(Close_Date_Most_Recent_Modified__c) as most_recent_close_date, #}
    recordtypeid as record_type_id,
    {# Record_Type_For_Report__c as record_type_name, #}
    {# partnerstack_lead_key__c as partner_stack_lead_key #}

from opps