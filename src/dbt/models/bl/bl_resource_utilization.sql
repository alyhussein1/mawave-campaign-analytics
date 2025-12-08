{{ config(
    materialized = 'table',
    tags = ["bl", "resources"]
) }}

WITH import_cl_employees AS (SELECT * FROM {{ ref('cl_employees') }}),
import_cl_time_tracking AS (SELECT * FROM {{ ref('cl_time_tracking') }}),
import_cl_clients AS (SELECT * FROM {{ ref('cl_clients') }}),

/* Aggregate time by employee and client */
employee_client_time AS (

    SELECT

        tt.employee_id,
        tt.client_id,
        COUNT(DISTINCT tt.report_date)                                  AS days_worked,
        SUM(tt.hours_worked)                                            AS total_hours,
        SUM(tt.cost_eur)                                                AS total_cost,
        SUM(CASE WHEN tt.is_productive THEN tt.hours_worked ELSE 0 END) AS productive_hours,
        MIN(tt.report_date)                                             AS first_work_date,
        MAX(tt.report_date)                                             AS last_work_date

    FROM import_cl_time_tracking tt
    GROUP BY 1, 2
),

/* Total hours by employee across all clients */
employee_totals AS (

    SELECT

        employee_id,
        SUM(hours_worked)   AS total_hours_all_clients,
        SUM(cost_eur)       AS total_cost_all_clients

    FROM import_cl_time_tracking
    GROUP BY employee_id
)

SELECT

    /* EMPLOYEE ATTRIBUTES */
    e.employee_id,
    e.employee_name,
    e.department_name,
    e.team_name,
    e.hourly_rate_eur,
    
    /* CLIENT ATTRIBUTES */
    ect.client_id,
    c.client_name,
    c.primary_industry,
    
    /* TIME METRICS */
    ect.days_worked,
    ect.total_hours,
    ect.total_cost,
    ect.productive_hours,
    ect.first_work_date,
    ect.last_work_date,
    
    /* UTILIZATION METRICS */
    CASE
        WHEN et.total_hours_all_clients > 0
        THEN ect.total_hours / et.total_hours_all_clients * 100
        ELSE NULL
    END AS pct_of_employee_time,
    
    CASE
        WHEN ect.total_hours > 0
        THEN ect.productive_hours / ect.total_hours * 100
        ELSE NULL
    END AS productivity_rate_pct,
    
    /* TOTAL EMPLOYEE METRICS */
    et.total_hours_all_clients,
    et.total_cost_all_clients

FROM employee_client_time ect
JOIN import_cl_employees e 
    ON ect.employee_id = e.employee_id
JOIN import_cl_clients c 
    ON ect.client_id = c.client_id
JOIN employee_totals et 
    ON ect.employee_id = et.employee_id
ORDER BY e.employee_id, ect.total_hours DESC