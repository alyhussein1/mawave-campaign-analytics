{{ config(
    materialized = 'table',
    tags = ["ol", "metrics"]
) }}

WITH import_campaigns AS (SELECT * FROM {{ ref('cl_campaigns') }}),
import_social_metrics AS (SELECT * FROM {{ ref('cl_social_metrics') }})

SELECT

    c.campaign_id,
    c.campaign_name,
    c.client_id,
    c.client_name,
    c.platform,
    c.campaign_status,
    c.start_date,
    c.daily_budget_eur,
    
    sm.report_date,
    DATE_TRUNC(sm.report_date, MONTH)              AS report_month,
    
    /* Metrics */
    sm.engaged_users,
    sm.new_followers,
    sm.impressions,
    sm.website_clicks,
    
    /* Simple calculations */
    SAFE_DIVIDE(sm.engaged_users, sm.impressions)  AS engagement_rate,
    SAFE_DIVIDE(sm.website_clicks, sm.impressions) AS click_rate

FROM import_campaigns c
LEFT JOIN import_social_metrics sm
    ON c.client_id = sm.client_id
    AND c.platform = sm.platform
    AND sm.report_date >= c.start_date
WHERE sm.report_date IS NOT NULL