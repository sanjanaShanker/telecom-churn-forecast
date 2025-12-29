-- check if data is loaded
SELECT * FROM churn_forecast_reporting LIMIT 10;

# Weighted Churn by Product Group
SELECT
    reporting_month,
    product_group,
    SUM(churn_value) AS total_churn,
    SUM(
        churn_value *
        CASE
            WHEN customer_type = 'regular payer' THEN 1.2
            WHEN customer_type = 'irregular payer' THEN 1.0
            ELSE 0.8
        END
    ) AS weighted_churn
FROM churn_forecast_reporting
WHERE reporting_kpi = 'ACTUAL'
GROUP BY reporting_month, product_group;

#Actual vs Forecast Comparison
WITH base AS (
    SELECT
        reporting_month,
        product_group,
        SUM(CASE WHEN reporting_kpi = 'ACTUAL' THEN churn_value END) AS actual_churn,
        SUM(CASE WHEN reporting_kpi = 'FORECAST' THEN churn_value END) AS forecast_churn
    FROM churn_forecast_reporting
    GROUP BY reporting_month, product_group
)
SELECT
    reporting_month,
    product_group,
    actual_churn,
    forecast_churn,
    (forecast_churn - actual_churn) AS delta,
    ROUND(
        ABS(forecast_churn - actual_churn) / NULLIF(actual_churn, 0) * 100,
        2
    ) AS error_percentage
FROM base;

# C. HIGH-RISK CUSTOMER & PRODUCT SEGMENTS
SELECT
    product_group,
    customer_type,
    SUM(churn_value) AS churn_volume
FROM churn_forecast_reporting
WHERE reporting_kpi = 'ACTUAL'
  AND customer_type IN ('move', 'churn')
GROUP BY product_group, customer_type
ORDER BY churn_volume DESC;

# Identify First Activity Month (Cohort)
WITH first_activity AS (
    SELECT
        product_group,
        customer_type,
        MIN(reporting_month) AS cohort_month
    FROM churn_forecast_reporting
    WHERE reporting_kpi = 'ACTUAL'
    GROUP BY product_group, customer_type
)
SELECT
    f.cohort_month,
    c.reporting_month,
    c.product_group,
    c.customer_type,
    SUM(c.churn_value) AS churn_value
FROM churn_forecast_reporting c
JOIN first_activity f
    ON c.product_group = f.product_group
   AND c.customer_type = f.customer_type
GROUP BY
    f.cohort_month,
    c.reporting_month,
    c.product_group,
    c.customer_type;

#3-Month Rolling Churn
SELECT
    reporting_month,
    product_group,
    SUM(churn_value) AS monthly_churn,
    SUM(SUM(churn_value)) OVER (
        PARTITION BY product_group
        ORDER BY reporting_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_month_churn
FROM churn_forecast_reporting
WHERE reporting_kpi = 'ACTUAL'
GROUP BY reporting_month, product_group;

