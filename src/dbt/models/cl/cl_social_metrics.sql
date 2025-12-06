{{ config(
    materialized = 'table',
    unique_key = 'client_id',
    tags = ["cl", "social_metrics"]
) }}

WITH import_il_social_metrics AS (SELECT * FROM {{ source('csv', 'social_metrics') }})

SELECT

    /* CLIENT ID & NAME */
    CAST(client_id AS STRING)   AS client_id,
    client_name,

    /* TIME DIMENSION */
    report_date,

    /* PLATFORM */
    platform,

    /* ENGAGEMENT METRICS */
    engaged_users,
    new_followers,
    total_followers,

    /* REACH METRICS */
    impressions,
    organic_impressions,

    /* PROFILE & WEBSITE METRICS */
    profile_views,
    website_clicks

FROM import_il_social_metrics
