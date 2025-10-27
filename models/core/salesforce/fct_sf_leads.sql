WITH leads AS (
    SELECT * FROM {{ ref('stg_sf_leads') }}
),
lead_history AS (
    SELECT * FROM {{ ref('stg_sf_lead_history') }}
    where life_cycle_stage in ('Status', 'csbs__Status_Detail__c','leadMerged')
),

stage_dates AS (
    SELECT
        lead_id,
        MIN(CASE WHEN new_value = '1st Attempt' THEN lead_stage_date END) AS first_attempt_date,
        MIN(CASE WHEN new_value = 'Working - Contacted' THEN lead_stage_date END) AS contacted_date,
        MIN(CASE WHEN new_value IN ('Hot Lead', 'Hot Email Lead') THEN lead_stage_date END) AS hot_lead_date,

        MIN(CASE WHEN new_value = 'Meeting Booked' THEN lead_stage_date END) AS meeting_booked_date,
        MIN(CASE WHEN new_value = 'Meeting Completed' THEN lead_stage_date END) AS meeting_completed_date,
        MIN(CASE WHEN new_value = 'Meeting No Show' THEN lead_stage_date END) AS meeting_no_show_date,

        MIN(CASE WHEN new_value IN ('Working - Application Out', 'Application Out') THEN lead_stage_date END) AS application_out_date,
        MIN(CASE WHEN new_value IN ('Awaiting Docs', 'Specialty - Awaiting Docs') THEN lead_stage_date END) AS awaiting_docs_date,
        MIN(CASE WHEN new_value = 'Docs In but No App' THEN lead_stage_date END) AS docs_in_no_app_date,
        MIN(CASE WHEN new_value = 'Credit Requested' THEN lead_stage_date END) AS credit_requested_date,

        MIN(CASE WHEN new_value IN ('Not Qualified', 'Does Not Qualify', 'DNQ - Negative Days', 'DNQ - Marketing') THEN lead_stage_date END) AS not_qualified_date,
        MIN(CASE WHEN new_value = 'Not Interested' THEN lead_stage_date END) AS not_interested_date,
        MIN(CASE WHEN new_value = 'Not Ready' THEN lead_stage_date END) AS not_ready_date,
        MIN(CASE WHEN new_value = 'Do Not Contact' THEN lead_stage_date END) AS do_not_contact_date,
        MIN(CASE WHEN new_value = 'Merchant Declined' THEN lead_stage_date END) AS merchant_declined_date,
        MIN(CASE WHEN new_value = 'No Business Bank Account' THEN lead_stage_date END) AS no_business_bank_date,

        MIN(CASE WHEN new_value IN ('Sent to Torro', 'Sent To Giggle', 'Sent to PFG',
                                    'Sent To  Low-Revenue Funding', 'Sent for SLOC',
                                    'Refer to 7 Figures Funding (Sub Status)') THEN lead_stage_date END) AS sent_to_partner_date,

        MIN(CASE WHEN new_value = 'Fraud' THEN lead_stage_date END) AS fraud_date,
        MIN(CASE WHEN new_value = 'False Information' THEN lead_stage_date END) AS false_info_date,
        MIN(CASE WHEN new_value = 'Duplicate' THEN lead_stage_date END) AS duplicate_date,

        MIN(CASE WHEN new_value = 'Closed - Converted' THEN lead_stage_date END) AS converted_date,
        MIN(CASE WHEN new_value = 'Future Client' THEN lead_stage_date END) AS future_client_date,
        MIN(CASE WHEN new_value = 'Closed - Not Converted' THEN lead_stage_date END) AS closed_not_converted_date
    FROM lead_history
    GROUP BY lead_id
)

SELECT
    l.lead_id,
    l.name,
    l.email,
    l.company,
    CASE
        WHEN l.lead_source IN ('BTC Website', 'Website', 'Web Form') THEN 'BTC Website'
        WHEN l.lead_source IN ('Affiliate - Meridian 102', 'Affiliate - Meridian') THEN 'Affiliate - Meridian'
        WHEN l.lead_source = 'Affiliate - Fundwise' THEN 'Affiliate - Fundwise'
        WHEN l.lead_source = 'Affiliate - Charlie Chang' THEN 'Affiliate - Charlie Chang'
        WHEN l.lead_source = 'Google Ads' THEN 'Google Ads'
        WHEN l.lead_source = 'TikTok Blue' THEN 'TikTok Ads'
        WHEN l.lead_source = 'Application Parser' THEN 'System / Integration'
        WHEN l.lead_source = 'Manual QA Test' THEN 'Internal / Test'
        WHEN l.lead_source IS NULL OR lead_source = 'null' THEN 'Unknown'
        ELSE l.lead_source
    END AS platform,
    l.industry,
    l.lead_status,
    l.lead_status_detail,
    converted_opportunity_id,
    lead_created_date,
    l.lead_converted_date,
    l.last_status_change_date,
    l.utm_term,
    l.utm_medium,
    l.utm_source,
    l.utm_content,
    l.utm_campaign,
    l.is_converted,
    s.* EXCEPT(lead_id),


    CASE WHEN s.converted_date IS NOT NULL THEN 1 ELSE 0 END AS Converted,
    CASE WHEN s.closed_not_converted_date IS NOT NULL THEN 1 ELSE 0 END AS Not_Converted,
    CASE WHEN s.meeting_booked_date IS NOT NULL THEN 1 ELSE 0 END AS had_meeting,
    CASE WHEN s.application_out_date IS NOT NULL THEN 1 ELSE 0 END AS submitted_application,
    CASE WHEN s.not_qualified_date IS NOT NULL 
       OR s.not_interested_date IS NOT NULL 
       OR s.do_not_contact_date IS NOT NULL THEN 1 ELSE 0 END AS is_disqualified

FROM leads l
LEFT JOIN stage_dates s ON l.lead_id = s.lead_id
