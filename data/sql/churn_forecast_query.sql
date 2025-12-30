-- check if the data is loaded correctly 
select * from churn_forecast_reporting;

-- MONTH-ON-MONTH CHANGE
WITH monthly AS (
    SELECT
        *,
        SUM(churn_value) AS monthly_churn
    FROM churn_forecast_reporting
    GROUP BY
        reporting_month, product_group, footprint, reporting_kpi
)
SELECT
    *,
    monthly_churn
    - LAG(monthly_churn) OVER (
        PARTITION BY product_group, footprint, reporting_kpi
        ORDER BY reporting_month
    ) AS mom_change
FROM monthly;

-- MONTH-WISE TREND STATUS
SELECT
    *,

    CASE
        WHEN LAG(monthly_churn) OVER (
                 PARTITION BY product_group, footprint, reporting_kpi
                 ORDER BY reporting_month
             ) IS NULL
        THEN 'NO PRIOR DATA'
        WHEN monthly_churn >
             LAG(monthly_churn) OVER (
                 PARTITION BY product_group, footprint, reporting_kpi
                 ORDER BY reporting_month
             )
        THEN 'INCREASE'
        WHEN monthly_churn <
             LAG(monthly_churn) OVER (
                 PARTITION BY product_group, footprint, reporting_kpi
                 ORDER BY reporting_month
             )
        THEN 'DECREASE'
        ELSE 'NO CHANGE'
    END AS trend_status

FROM monthly
ORDER BY reporting_month;

-- ROLLING 3-MONTH CHURN
SELECT
    *,

    SUM(monthly_churn) OVER (
        PARTITION BY product_group, footprint, reporting_kpi
        ORDER BY reporting_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_month_churn

FROM monthly
ORDER BY reporting_month;

-- RELATIVE CHURN CONTRIBUTION %
SELECT
    *,

    ROUND(
        monthly_churn
        /
        NULLIF(
            SUM(monthly_churn) OVER (
                PARTITION BY reporting_month, reporting_kpi
            ),
            0
        ),
        4
    ) AS contribution_percentage

FROM monthly
ORDER BY reporting_month;

-- NORMALIZED CHURN INDEX
SELECT
    *,

    ROUND(
        monthly_churn
        /
        AVG(monthly_churn) OVER (
            PARTITION BY reporting_month, reporting_kpi
        ),
        2
    ) AS normalized_churn_index

FROM monthly
ORDER BY reporting_month;

-- FORECAST vs ACTUAL DELTA
SELECT
    *,

    SUM(CASE WHEN reporting_kpi = 'FORECAST' THEN monthly_churn END)
    -
    SUM(CASE WHEN reporting_kpi = 'ACTUAL' THEN monthly_churn END)
    AS forecast_actual_delta

FROM monthly
GROUP BY
    reporting_month,
    product_group,
    footprint
ORDER BY reporting_month;

-- FORECAST VOLATILITY
SELECT
    *,

    ABS(
        churn_value
        -
        LAG(churn_value) OVER (
            PARTITION BY reporting_month, product_group, footprint
            ORDER BY ingestion_date
        )
    ) AS forecast_volatility

FROM churn_forecast_reporting
WHERE reporting_kpi = 'FORECAST'
ORDER BY reporting_month, ingestion_date;

-- HIGH-RISK MONTH IDENTIFICATION
SELECT
    *,

    CASE
        WHEN monthly_churn >
             AVG(monthly_churn) OVER (
                 PARTITION BY product_group, footprint, reporting_kpi
             )
        THEN 'HIGH RISK'
        ELSE 'NORMAL'
    END AS risk_flag

FROM monthly
ORDER BY reporting_month;

# Weighted Churn by Product Group
SELECT
    *,
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

#  HIGH-RISK CUSTOMER & PRODUCT SEGMENTS
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
