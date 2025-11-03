with google_ads_accounts as (
    select * from {{ source("Google_Ads", "account_performance_report") }}
)

select distinct 
customer_id as account_id,
customer_descriptive_name as account_name

from  google_ads_accounts