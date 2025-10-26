WITH cte as (
    select *
    from {{source('Salesforce', 'LeadHistory')}}
)

SELECT
    Id,
    Field as life_cycle_stage,
    LeadId as lead_id,
    NewValue as new_value,
    OldValue as old_value,
    date(createddate) as lead_stage_date
    FROM cte
