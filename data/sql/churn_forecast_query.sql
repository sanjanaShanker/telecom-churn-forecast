-- check if the data is loaded correctly 
select * from churn_forecast_reporting;

-- MONTH-ON-MONTH CHANGE
WITH monthly AS (
    SELECT
        reporting_month,
        product_group,
        footprint,
        reporting_kpi,
        SUM(churn_value) AS monthly_churn
    FROM churn_forecast_reporting
    GROUP BY
        reporting_month, product_group, footprint, reporting_kpi
)
SELECT
    reporting_month,
    product_group,
    footprint,
    reporting_kpi,
    monthly_churn,
    monthly_churn
    - LAG(monthly_churn) OVER (
        PARTITION BY product_group, footprint, reporting_kpi
        ORDER BY reporting_month
    ) AS mom_change
FROM monthly;

-- MONTH-WISE TREND STATUS

SELECT
    reporting_month,
    product_group,
    footprint,
    reporting_kpi,
    monthly_churn,

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
    reporting_month,
    product_group,
    footprint,
    reporting_kpi,
    monthly_churn,

    SUM(monthly_churn) OVER (
        PARTITION BY product_group, footprint, reporting_kpi
        ORDER BY reporting_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_month_churn

FROM monthly
ORDER BY reporting_month;

-- RELATIVE CHURN CONTRIBUTION %
SELECT
    reporting_month,
    product_group,
    footprint,
    reporting_kpi,
    monthly_churn,

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
    reporting_month,
    product_group,
    footprint,
    reporting_kpi,
    monthly_churn,

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
    reporting_month,
    product_group,
    footprint,

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
    reporting_month,
    ingestion_date,
    product_group,
    footprint,
    churn_value,

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
    reporting_month,
    product_group,
    footprint,
    reporting_kpi,
    monthly_churn,

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
