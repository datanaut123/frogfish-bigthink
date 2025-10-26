with tasks as (
    select * from {{ source("Salesforce", "Task") }}
)

select
    id as task_id,
    date(createddate) as date,
    date(activitydate) as activity_date,
    subject as task_subject,
    tasksubtype as task_subtype,
    calltype as call_type,
    calldisposition as call_disposition,
    isclosed as is_closed,
    status,
    whoid as contact_id

from tasks