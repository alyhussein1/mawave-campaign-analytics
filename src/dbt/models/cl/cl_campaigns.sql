{{ config(
    materialized = 'table',
    unique_key = 'campaign_id',
    tags = ["cl", "campaigns"]
) }}

WITH import_il_campaigns AS (SELECT * FROM {{ source('csv', 'campaigns') }})

SELECT

    /* CAMPAIGN & CLIENT IDENTIFIERS */
    CAST(campaign_id AS STRING)             AS campaign_id,
    CAST(client_id AS STRING)               AS client_id,
    client_name,

    /* CAMPAIGN TIMELINE */
    DATE(start_date)                        AS start_date,

    /* CAMPAIGN ATTRIBUTES */
    campaign_name,
    platform,
    campaign_status,
    CAST(daily_budget_eur AS INT64)         AS daily_budget_eur

FROM import_il_campaigns
