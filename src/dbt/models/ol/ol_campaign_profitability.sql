{{ config(
    materialized = 'table',
    unique_key = 'campaign_id',
    tags = ["ol", "campaigns"]
) }}

WITH import_cl_campaigns AS (SELECT * FROM {{ ref('cl_campaigns') }}),
import_cl_ad_metrics AS (SELECT * FROM {{ ref('cl_ad_metrics') }}),
import_cl_clients AS (SELECT * FROM {{ ref('cl_clients') }}),
import_cl_time_tracking AS (SELECT * FROM {{ ref('cl_time_tracking') }}),

/* Aggregate ad spend and revenue by campaign */
campaign_spend AS (

    SELECT

        campaign_id,
        client_id,
        SUM(spend_eur)      AS total_ad_spend,
        SUM(revenue_eur)    AS total_revenue,
        SUM(conversions)    AS total_conversions

    FROM ad_metrics
    WHERE attribution_window = '7d_click'
    GROUP BY campaign_id, client_id
),

/* Aggregate internal costs by client - productive hours only */
client_time_costs AS (

    SELECT

        client_id,
        SUM(cost_eur)       AS total_internal_cost,
        SUM(hours_worked)   AS total_hours

    FROM time_tracking
    WHERE is_productive = TRUE
    GROUP BY client_id
)

SELECT

    /* CAMPAIGN ATTRIBUTES */
    cs.campaign_id,
    cam.campaign_name,
    cs.client_id,
    c.client_name,
    cam.platform,
    cam.campaign_status,
    
    /* PERFORMANCE METRICS */
    cs.total_ad_spend,
    cs.total_revenue,
    cs.total_conversions,
    
    /* INTERNAL COSTS */
    ctc.total_internal_cost,
    ctc.total_hours,
    
    /* PROFITABILITY METRICS*/
    cs.total_ad_spend + COALESCE(ctc.total_internal_cost, 0) AS total_cost,
    cs.total_revenue - (cs.total_ad_spend + COALESCE(ctc.total_internal_cost, 0)) AS gross_profit,
    
    CASE
        WHEN cs.total_ad_spend + COALESCE(ctc.total_internal_cost, 0) > 0
        THEN (cs.total_revenue - (cs.total_ad_spend + COALESCE(ctc.total_internal_cost, 0)))
             / (cs.total_ad_spend + COALESCE(ctc.total_internal_cost, 0)) * 100
        ELSE NULL
    END AS profit_margin_pct

FROM campaign_spend cs
JOIN campaigns cam 
    ON cs.campaign_id = cam.campaign_id
JOIN clients c 
    ON cs.client_id = c.client_id
LEFT JOIN client_time_costs ctc 
    ON cs.client_id = ctc.client_id
ORDER BY cs.campaign_id