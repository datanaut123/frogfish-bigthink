with opps as (select * from {{ source("Salesforce", "OpportunityStage") }})

select distinct 
    id as stage_id,
    masterlabel as opportunity_stage

from opps