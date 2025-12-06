{{ config(
    materialized = 'table',
    unique_key = 'client_id',
    tags = ["bl", "dashboard"]
) }}

WITH

clients AS (SELECT * FROM {{ ref('cl_clients') }}),
campaign_profitability AS (SELECT * FROM {{ ref('ol_campaign_profitability') }}),
client_projects AS (SELECT * FROM {{ ref('ol_client_projects_with_time') }}),

/* Aggregate campaign metrics by client */
client_campaign_summary AS (

    SELECT

        client_id,
        COUNT(DISTINCT campaign_id)                                                 AS total_campaigns,
        COUNT(DISTINCT CASE WHEN campaign_status = 'ACTIVE' THEN campaign_id END)   AS active_campaigns,
        SUM(total_ad_spend)                                                         AS total_ad_spend,
        SUM(total_revenue)                                                          AS total_revenue,
        SUM(total_conversions)                                                      AS total_conversions,
        SUM(total_internal_cost)                                                    AS total_internal_cost,
        SUM(total_cost)                                                             AS total_cost,
        SUM(gross_profit)                                                           AS total_profit,
        AVG(profit_margin_pct)                                                      AS avg_profit_margin_pct
    
    FROM campaign_profitability
    GROUP BY client_id
),

/* Aggregate project metrics by client */
client_project_summary AS (

    SELECT

        client_id,
        COUNT(DISTINCT project_id)                                                      AS total_projects,
        COUNT(DISTINCT CASE WHEN project_status = 'In Progress' THEN project_id END)    AS active_projects,
        SUM(total_budget_eur)                                                           AS total_project_budget

    FROM client_projects
    GROUP BY client_id
)

SELECT

    /* CLIENT ATTRIBUTES */
    c.client_id,
    c.client_name,
    c.primary_industry,
    c.secondary_industry,
    c.country,
    
    /* CAMPAIGN METRICS */
    COALESCE(cs.total_campaigns, 0)             AS total_campaigns,
    COALESCE(cs.active_campaigns, 0)            AS active_campaigns,
    COALESCE(cs.total_ad_spend, 0)              AS total_ad_spend,
    COALESCE(cs.total_revenue, 0)               AS total_revenue,
    COALESCE(cs.total_conversions, 0)           AS total_conversions,
    
    /* COST & PROFITABILITY */
    COALESCE(cs.total_internal_cost, 0)         AS total_internal_cost,
    COALESCE(cs.total_cost, 0)                  AS total_cost,
    COALESCE(cs.total_profit, 0)                AS total_profit,
    cs.avg_profit_margin_pct,
    
    /* PROJECT METRICS */
    COALESCE(ps.total_projects, 0)              AS total_projects,
    COALESCE(ps.active_projects, 0)             AS active_projects,
    COALESCE(ps.total_project_budget, 0)        AS total_project_budget,
    
    /* CLIENT STATUS */
    CASE
        WHEN cs.active_campaigns > 0 THEN 'Active'
        WHEN cs.total_campaigns > 0 THEN 'Inactive'
        ELSE 'No Campaigns'
    END AS client_status,
    
    CASE
        WHEN cs.total_profit > 0 THEN 'Profitable'
        WHEN cs.total_profit = 0 THEN 'Break-even'
        WHEN cs.total_profit < 0 THEN 'Loss-making'
        ELSE 'No Data'
    END AS profitability_status

FROM clients c
LEFT JOIN client_campaign_summary cs ON c.client_id = cs.client_id
LEFT JOIN client_project_summary ps ON c.client_id = ps.client_id
ORDER BY c.client_id