with accounts as (
    select * from {{ source("Salesforce", "Account") }}
)

select
    id as account_id,
    name as account_name,
    type as account_type,
    csbs__Email__c as email,
    ownerid as owner_id,
    industry,
    billingcountry as billing_country,
    shippingcountry as shipping_country,
    date(createddate) as account_create_date,
    recordtypeid as record_type,
    csbs__Number_of_Locations__c as number_of_locations,
    status__c as account_status

from accounts