{{ config(
    materialized = 'table',
    tags = ["ol", "metrics"]
) }}

WITH import_cl_campaigns AS (SELECT * FROM {{ ref('cl_campaigns') }}),
import_cl_clients AS (SELECT * FROM {{ ref('cl_clients') }}),
import_cl_ad_metrics AS (SELECT * FROM {{ ref('cl_ad_metrics') }})

SELECT

    /* TIME DIMENSION */
    am.report_date,
    am.attribution_window,

    /* CAMPAIGN & CLIENT IDENTIFIERS */
    am.campaign_id,
    cam.campaign_name,
    cam.campaign_status,
    cam.platform,
    am.client_id,
    c.client_name,
    c.primary_industry,

    /* PERFORMANCE METRICS */
    am.spend_eur,
    am.impressions,
    am.clicks,
    am.conversions,
    am.revenue_eur,

    /* CALCULATED EFFICIENCY METRICS */
    am.cpm,
    am.ctr,
    am.cvr,
    am.cpc,
    am.cpa,
    am.roas,

    /* PROFITABILITY METRICS */
    am.revenue_eur - am.spend_eur AS profit_eur,
    SAFE_DIVIDE(am.revenue_eur - am.spend_eur, am.spend_eur) * 100 AS profit_margin_pct,

    /* PERFORMANCE INDICATORS */
    CASE
        WHEN am.roas >= 4 THEN 'Excellent'
        WHEN am.roas >= 2 THEN 'Good'
        WHEN am.roas >= 1 THEN 'Marginal'
        ELSE 'Poor'
    END AS performance_tier,

    CASE
        WHEN am.revenue_eur > am.spend_eur THEN 'Profitable'
        WHEN am.revenue_eur = am.spend_eur THEN 'Break-even'
        ELSE 'Loss'
    END AS profitability_status

FROM import_cl_ad_metrics am
JOIN import_cl_campaigns cam 
    ON am.campaign_id = cam.campaign_id
JOIN import_cl_clients c 
    ON am.client_id = c.client_id
/* Filtering on default attribution window */ 
WHERE am.attribution_window = '7d_click'
ORDER BY report_date, campaign_id