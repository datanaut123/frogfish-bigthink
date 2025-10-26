with opps as (select * from {{ source("Salesforce", "Opportunity") }})

select
    id as opportunity_id,
    name as opportunity_name,
    TRIM(SPLIT(name, '|')[OFFSET(0)]) AS company,
    OwnerId as owner_id,
    amount as opportunity_amount,
    accountid as account_id,
    LeadSource as lead_source,
    StageName as opportunity_stage,
    contactid as contact_id,
    date(createddate) as opportunity_created_date,
    date(CloseDate) as close_date,
    recordtypeid as record_type_id,

from opps