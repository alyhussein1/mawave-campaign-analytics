{{ config(
    materialized = 'table',
    unique_key = 'employee_id',
    tags = ["cl", "time_tracking"]
) }}

WITH import_il_time_tracking AS (SELECT * FROM {{ source('gsheets', 'time_tracking') }})

SELECT

    CAST(employee_id AS STRING)       AS employee_id,
    employee_name,
    CAST(client_id AS STRING)         AS client_id,
    client_name,
    DATE(report_date)                 AS report_date,
    CAST(hours_worked AS NUMERIC)     AS hours_worked,
    department_name,
    CAST(cost_eur AS NUMERIC)         AS cost_eur,
    is_productive

FROM import_il_time_tracking
