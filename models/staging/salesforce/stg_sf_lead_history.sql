with
    cte_1 as (
        select
            leadid as lead_id,
            newvalue as lead_stage,
            date(createddate) as lead_stage_date,
            row_number() over (
                partition by leadid, newvalue order by createddate asc
            ) as rn
        from {{ source('Salesforce', 'LeadHistory') }}
        where field in ('Status') and newvalue <> 'Hot Lead'
        -- qualify rn = 1
    ),

    cte_2 as (
        select
            leadid as lead_id,
            oldvalue as lead_stage,
            date(createddate) as lead_stage_date,
            row_number() over (
                partition by leadid, newvalue order by createddate asc
            ) as rn
        from {{ source('Salesforce', 'LeadHistory') }}
        where field in ('Status') and oldvalue = 'Hot Lead'
       --  qualify rn = 1

    )

select lead_id, lead_stage, lead_stage_date

from cte_1

union all

select lead_id, lead_stage, lead_stage_date

from cte_2
