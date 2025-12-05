{{ config(
    materialized = 'table',
    unique_key = 'project_id',
    tags = ["ol", "projects"]
) }}

WITH import_il_projects AS (SELECT * FROM {{ ref('cl_projects') }}),
import_il_clients AS (SELECT * FROM {{ ref('cl_clients') }})

SELECT

    /* PROJECT ATTRIBUTES */
    p.project_id,
    p.project_name,
    p.client_id,
    c.client_name,
    c.primary_industry,
    c.secondary_industry,
    c.country,
    
    /* PROJECT TINELINE */
    p.project_start_date,
    p.project_end_date,
    DATE_DIFF(p.project_end_date, p.project_start_date, DAY)                        AS project_duration_days,
    DATE_DIFF(p.project_end_date, p.project_start_date, MONTH)                      AS project_duration_months,
    
    /* BUDGET INFORMATION */
    p.monthly_budget_eur,
    p.monthly_budget_eur * DATE_DIFF(p.project_end_date, p.project_start_date, MONTH) AS total_budget_eur,
    
    /* PROJECT STATUS */
    CASE 
        WHEN CURRENT_DATE() < p.project_start_date THEN 'Not Started'
        WHEN CURRENT_DATE() BETWEEN p.project_start_date AND p.project_end_date THEN 'In Progress'
        WHEN CURRENT_DATE() > p.project_end_date THEN 'Completed'
    END AS project_status

FROM import_il_projects p
JOIN import_il_clients c 
    ON p.client_id = c.client_id
ORDER BY p.project_id