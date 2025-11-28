/*
================================================
ELT Pipeline - TRANSFORM Phase (Analytics Mart)
Daily Sales Summary

Final transformation layer producing analytics-ready data.
All transformations run INSIDE Snowflake (ELT approach).
================================================
*/

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

daily_aggregates AS (
    SELECT
        order_date,
        
        -- Order metrics
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        
        -- Revenue metrics
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value,
        MIN(total_amount) AS min_order_value,
        MAX(total_amount) AS max_order_value,
        
        -- Order tier breakdown
        SUM(CASE WHEN order_tier = 'HIGH_VALUE' THEN 1 ELSE 0 END) AS high_value_orders,
        SUM(CASE WHEN order_tier = 'MEDIUM_VALUE' THEN 1 ELSE 0 END) AS medium_value_orders,
        SUM(CASE WHEN order_tier = 'LOW_VALUE' THEN 1 ELSE 0 END) AS low_value_orders,
        
        -- Status breakdown
        SUM(CASE WHEN order_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders,
        SUM(CASE WHEN order_status = 'PENDING' THEN 1 ELSE 0 END) AS pending_orders,
        SUM(CASE WHEN order_status = 'SHIPPED' THEN 1 ELSE 0 END) AS shipped_orders
        
    FROM orders
    GROUP BY order_date
),

final AS (
    SELECT
        order_date,
        DAYNAME(order_date) AS day_of_week,
        total_orders,
        unique_customers,
        ROUND(total_revenue, 2) AS total_revenue,
        ROUND(avg_order_value, 2) AS avg_order_value,
        high_value_orders,
        medium_value_orders,
        low_value_orders,
        completed_orders,
        pending_orders,
        shipped_orders,
        ROUND(completed_orders * 100.0 / NULLIF(total_orders, 0), 1) AS completion_rate,
        CURRENT_TIMESTAMP() AS _created_at
    FROM daily_aggregates
)

SELECT * FROM final
ORDER BY order_date
