{{ config(
    materialized = 'table',
    tags = ["ol", "projects"]
) }}

WITH import_projects AS (SELECT * FROM {{ ref('cl_projects') }}),
import_time_tracking AS (SELECT * FROM {{ ref('cl_time_tracking') }})

SELECT

    p.project_id,
    p.project_name,
    p.client_id,
    p.client_name,
    p.project_start_date,
    p.project_end_date,
    p.monthly_budget_eur,

    /* Time aggregations */
    COUNT(DISTINCT tt.employee_id) AS employee_count,
    SUM(tt.hours_worked) AS total_hours,
    SUM(CASE WHEN tt.is_productive THEN tt.hours_worked ELSE 0 END) AS productive_hours,

    /* Cost aggregations */
    SUM(tt.cost_eur) AS total_cost_eur,

    /* Productivity Rate Calculation */
    SAFE_DIVIDE(
        SUM(CASE WHEN tt.is_productive THEN tt.hours_worked ELSE 0 END),
        SUM(tt.hours_worked)
    ) AS productivity_rate

FROM import_projects p
LEFT JOIN import_time_tracking tt
    ON p.client_id = tt.client_id
    AND tt.report_date BETWEEN p.project_start_date AND COALESCE(p.project_end_date, CURRENT_DATE())

GROUP BY ALL
