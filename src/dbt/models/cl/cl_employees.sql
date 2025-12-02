{{ config(
    materialized = 'table',
    unique_key = 'employee_id',
    tags = ["cl", "employees"]
) }}

WITH import_il_employees AS (SELECT * FROM {{ source('gsheets', 'employees') }})

SELECT

    CAST(employee_id AS STRING)        AS employee_id,
    employee_name,
    department_name,
    team_name,
    CAST(hourly_rate_eur AS NUMERIC)   AS hourly_rate_eur,
    status

FROM import_il_employees
