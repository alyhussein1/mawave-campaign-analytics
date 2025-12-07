{{ config(
    materialized = 'table',
    tags = ["bl", "cohorts"]
) }}

WITH import_ol_unified_ad_metrics AS (SELECT * FROM {{ ref('ol_unified_ad_metrics') }}),

/* Aggregate metrics by client and month */
monthly_metrics AS (

    SELECT

        client_id,
        client_name,
        primary_industry,
        DATE_TRUNC(report_date, MONTH)  AS month,
        COUNT(DISTINCT campaign_id)     AS num_campaigns,
        SUM(spend_eur)                  AS total_spend,
        SUM(revenue_eur)                AS total_revenue,
        SUM(impressions)                AS total_impressions,
        SUM(clicks)                     AS total_clicks,
        SUM(conversions)                AS total_conversions,
        AVG(ctr)                        AS avg_ctr,
        AVG(cvr)                        AS avg_cvr,
        AVG(roas)                       AS avg_roas

    FROM import_ol_unified_ad_metrics
    GROUP BY client_id, client_name, primary_industry, DATE_TRUNC(report_date, MONTH)
),

/* Calculate month-over-month changes */
monthly_with_lag AS (

    SELECT

        *,
        LAG(total_spend) OVER (PARTITION BY client_id ORDER BY month) AS prev_month_spend,
        LAG(total_revenue) OVER (PARTITION BY client_id ORDER BY month) AS prev_month_revenue,
        LAG(total_conversions) OVER (PARTITION BY client_id ORDER BY month) AS prev_month_conversions

    FROM monthly_metrics
)

SELECT

    /* CLIENT ATTRIBUTES & TIME PERIOD */
    client_id,
    client_name,
    primary_industry,
    month,
    EXTRACT(YEAR FROM month)    AS year,
    EXTRACT(MONTH FROM month)   AS month_number,
    
    /* CAMPAIGN METRICS */
    num_campaigns,
    total_spend,
    total_revenue,
    total_impressions,
    total_clicks,
    total_conversions,
    
    /* PERFORMANCE METRICS */
    avg_ctr,
    avg_cvr,
    avg_roas,
    
    /* PROFITABILITY */
    total_revenue - total_spend AS profit,
    CASE
        WHEN total_spend > 0
        THEN (total_revenue - total_spend) / total_spend * 100
        ELSE NULL
    END AS profit_margin_pct,
    
    /* MONTH-OVER-MONTH GROWTH */
    CASE
        WHEN prev_month_spend > 0
        THEN (total_spend - prev_month_spend) / prev_month_spend * 100
        ELSE NULL
    END AS spend_growth_pct,
    
    CASE
        WHEN prev_month_revenue > 0
        THEN (total_revenue - prev_month_revenue) / prev_month_revenue * 100
        ELSE NULL
    END AS revenue_growth_pct,
    
    CASE
        WHEN prev_month_conversions > 0
        THEN (total_conversions - prev_month_conversions) / prev_month_conversions * 100
        ELSE NULL
    END AS conversion_growth_pct,
    
    /* COHORT CLASSIFICATION */
    CASE
        WHEN total_revenue - total_spend > 0 THEN 'Profitable'
        WHEN total_revenue - total_spend = 0 THEN 'Break-even'
        ELSE 'Loss-making'
    END AS profitability_status,
    
    CASE
        WHEN avg_roas >= 3 THEN 'High Performer'
        WHEN avg_roas >= 1.5 THEN 'Average Performer'
        ELSE 'Low Performer'
    END AS performance_cohort

FROM monthly_with_lag
ORDER BY client_id, month DESC