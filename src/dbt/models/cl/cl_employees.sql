{{ config(
    materialized = 'table',
    unique_key = 'employee_id',
    tags = ["cl", "employees"]
) }}

WITH import_il_employees AS (SELECT * FROM {{ source('csv', 'employees') }})

SELECT

    /* EMPLOYEE ID & NAME */
    CAST(employee_id AS STRING)        AS employee_id,
    employee_name,

    /* ORGANIZATIONAL ATTRIBUTES */
    department_name,
    team_name,

    /* COMPENSATION */
    CAST(hourly_rate_eur AS INT64)     AS hourly_rate_eur,

    /* EMPLOYMENT STATUS */
    status

FROM import_il_employees
