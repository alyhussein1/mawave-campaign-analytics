{{ config(
    materialized = 'table',
    unique_key = 'project_id',
    tags = ["cl", "projects"]
) }}

WITH import_il_projects AS (SELECT * FROM {{ source('csv', 'projects') }})

SELECT

    /* PROJECT IDENTIFIERS */
    CAST(project_id AS STRING)            AS project_id,

    /* CLIENT ID & NAME */
    CAST(client_id AS STRING)             AS client_id,
    client_name,

    /* PROJECT ATTRIBUTES */
    project_name,

    /* PROJECT TIMELINE */
    project_start_date                    AS project_start_date,
    project_end_date                      AS project_end_date,

    /* BUDGET INFORMATION */
    CAST(monthly_budget_eur AS NUMERIC)   AS monthly_budget_eur

FROM import_il_projects
