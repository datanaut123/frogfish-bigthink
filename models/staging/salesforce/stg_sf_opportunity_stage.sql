with opps as (select * from {{ source("Salesforce", "OpportunityStage") }})

select
    id as stage_id,
    masterlabel as stage_status,
    Date(CreatedDate) as opportunity_stage_change_date,

from opps