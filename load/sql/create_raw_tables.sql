/*
================================================
ELT Pipeline - LOAD Phase
Create Raw Tables in Snowflake

In ELT, we LOAD raw data directly into the warehouse
without transformation. Transformations happen AFTER loading.
================================================
*/

-- Create database for ELT pipeline
CREATE DATABASE IF NOT EXISTS ECOMMERCE_ELT;
USE DATABASE ECOMMERCE_ELT;

-- Create schemas following ELT architecture
CREATE SCHEMA IF NOT EXISTS RAW;          -- Raw data (as extracted)
CREATE SCHEMA IF NOT EXISTS STAGING;      -- Cleaned/standardized data
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE; -- Business logic
CREATE SCHEMA IF NOT EXISTS MARTS;        -- Final analytics tables

-- ============================================
-- RAW LAYER TABLES (LOAD phase destination)
-- These tables receive data exactly as extracted
-- ============================================

-- Raw Orders Table
CREATE OR REPLACE TABLE RAW.RAW_ORDERS (
    order_id INTEGER,
    customer_id VARCHAR(50),
    order_date VARCHAR(50),      -- Keep as string, transform later
    total_amount FLOAT,
    status VARCHAR(50),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Raw Customers Table
CREATE OR REPLACE TABLE RAW.RAW_CUSTOMERS (
    customer_id VARCHAR(50),
    name VARCHAR(200),
    email VARCHAR(200),
    created_at VARCHAR(50),      -- Keep as string, transform later
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Raw Products Table
CREATE OR REPLACE TABLE RAW.RAW_PRODUCTS (
    product_id VARCHAR(50),
    name VARCHAR(200),
    category VARCHAR(100),
    price FLOAT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- FILE FORMAT for CSV loading
-- ============================================
CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '');

-- ============================================
-- STAGE for file uploads
-- ============================================
CREATE OR REPLACE STAGE RAW.ELT_STAGE
    FILE_FORMAT = RAW.CSV_FORMAT;

-- ============================================
-- SAMPLE DATA LOAD (for demonstration)
-- In production, use COPY INTO from stage
-- ============================================
INSERT INTO RAW.RAW_ORDERS (order_id, customer_id, order_date, total_amount, status) VALUES
(1001, 'C001', '2024-01-15', 299.99, 'completed'),
(1002, 'C002', '2024-01-15', 149.50, 'completed'),
(1003, 'C001', '2024-01-16', 599.00, 'pending'),
(1004, 'C003', '2024-01-16', 89.99, 'completed'),
(1005, 'C004', '2024-01-17', 1299.00, 'shipped');

INSERT INTO RAW.RAW_CUSTOMERS (customer_id, name, email, created_at) VALUES
('C001', 'John Smith', 'john@email.com', '2023-06-01'),
('C002', 'Jane Doe', 'jane@email.com', '2023-07-15'),
('C003', 'Bob Wilson', 'bob@email.com', '2023-08-20'),
('C004', 'Alice Brown', 'alice@email.com', '2023-09-10');

INSERT INTO RAW.RAW_PRODUCTS (product_id, name, category, price) VALUES
('P001', 'Laptop Pro', 'Electronics', 999.99),
('P002', 'Wireless Mouse', 'Electronics', 49.99),
('P003', 'Office Chair', 'Furniture', 299.00),
('P004', 'Desk Lamp', 'Home', 39.99);

-- Verify data loaded
SELECT 'Orders' as table_name, COUNT(*) as row_count FROM RAW.RAW_ORDERS
UNION ALL
SELECT 'Customers', COUNT(*) FROM RAW.RAW_CUSTOMERS
UNION ALL
SELECT 'Products', COUNT(*) FROM RAW.RAW_PRODUCTS;
