{{ config(
    materialized = 'table',
    tags = ["ol", "profitability"]
) }}

WITH import_campaigns AS (SELECT * FROM {{ ref('cl_campaigns') }}),
import_time_tracking AS (SELECT * FROM {{ ref('cl_time_tracking') }})

SELECT

    c.campaign_id,
    c.campaign_name,
    c.client_id,
    c.client_name,
    c.platform,
    c.campaign_status,
    c.start_date,
    c.daily_budget_eur,

    tt.report_date AS cost_date,

    /* Labor costs for the day */
    SUM(tt.hours_worked) AS hours_worked,
    SUM(tt.cost_eur) AS labor_cost_eur,
    COUNT(DISTINCT tt.employee_id) AS employee_count,

    /* Ad spend for the day */
    c.daily_budget_eur AS daily_ad_spend_eur,

    /* Total daily cost */
    c.daily_budget_eur + SUM(tt.cost_eur) AS total_daily_cost_eur

FROM import_campaigns c
LEFT JOIN import_time_tracking tt
    ON c.client_id = tt.client_id
    AND tt.report_date >= c.start_date

GROUP BY ALL
