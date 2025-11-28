/*
================================================
ELT Pipeline - TRANSFORM Phase (Staging Layer)
Staged Orders Model

This is where ELT differs from ETL:
- Data is ALREADY in Snowflake (loaded in LOAD phase)
- We transform INSIDE the warehouse using SQL
- Snowflake's compute power handles transformations
================================================
*/

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    -- Read from RAW layer (data loaded in LOAD phase)
    SELECT * FROM {{ source('raw', 'raw_orders') }}
),

staged AS (
    SELECT
        -- Clean and standardize data
        order_id,
        customer_id,
        
        -- TRANSFORM: Convert string date to proper DATE type
        TRY_TO_DATE(order_date) AS order_date,
        
        -- TRANSFORM: Round amounts
        ROUND(total_amount, 2) AS total_amount,
        
        -- TRANSFORM: Standardize status to uppercase
        UPPER(TRIM(status)) AS order_status,
        
        -- Add derived fields
        CASE 
            WHEN total_amount >= 500 THEN 'HIGH_VALUE'
            WHEN total_amount >= 100 THEN 'MEDIUM_VALUE'
            ELSE 'LOW_VALUE'
        END AS order_tier,
        
        -- Metadata
        _loaded_at,
        CURRENT_TIMESTAMP() AS _transformed_at
        
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM staged
