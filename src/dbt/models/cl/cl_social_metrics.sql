{{ config(
    materialized = 'table',
    unique_key = 'client_id',
    tags = ["cl", "social_metrics"]
) }}

WITH import_il_social_metrics AS (SELECT * FROM {{ source('csv', 'social_metrics') }})

SELECT

    CAST(client_id AS STRING)   AS client_id,
    client_name,
    report_date,
    platform,
    engaged_users,
    new_followers,
    total_followers,
    impressions,
    organic_impressions,
    profile_views,
    website_clicks

FROM import_il_social_metrics
