{{ config(
    materialized = 'table',
    unique_key = 'campaign_id',
    tags = ["cl", "ad_metrics"]
) }}

WITH import_ad_metrics AS (SELECT * FROM {{ source('csv', 'ad_metrics') }})

SELECT

    CAST(campaign_id AS STRING)                 AS campaign_id,
    CAST(client_id AS STRING)                   AS client_id,
    client_name,
    platform,
    DATE(report_date)                           AS report_date,
    attribution_window,

    -- Performance Metrics
    CAST(spend_eur AS NUMERIC)                  AS spend_eur,
    CAST(impressions AS NUMERIC)                AS impressions,
    CAST(clicks AS INT64)                       AS clicks,
    CAST(conversions AS INT64)                  AS conversions,

    CAST(cpm AS NUMERIC)                        AS cpm,
    CAST(ctr AS NUMERIC)                        AS ctr,
    CAST(cvr AS NUMERIC)                        AS cvr,
    CAST(cpc AS NUMERIC)                        AS cpc,
    CAST(cpa AS NUMERIC)                        AS cpa,

    CAST(revenue_eur AS NUMERIC)                AS revenue_eur,
    CAST(roas AS NUMERIC)                       AS roas

FROM import_ad_metrics
