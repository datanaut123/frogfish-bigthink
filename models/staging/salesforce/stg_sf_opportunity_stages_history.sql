with
    opportunity_stages as (
        select
            id as id,
            amount as opportunity_amount,
            date(closedate) as opportunity_close_date,
            stagename as opportunity_stage,
            prevamount as prev_opportunity_amount,
            createddate as opportunity_stage_change_date,
            opportunityid as opportunity_id,
            row_number() over (
                partition by opportunityid, stagename order by createddate asc
            ) as rn

        from {{ source("Salesforce", "OpportunityHistory") }}
    )

select
    id,
    opportunity_amount,
    opportunity_close_date,
    opportunity_stage,
    prev_opportunity_amount,
    opportunity_stage_change_date,
    opportunity_id

from opportunity_stages
where rn = 1