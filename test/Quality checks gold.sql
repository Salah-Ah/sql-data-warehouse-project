/*
================================================================================
Quality Checks
================================================================================

Script Purpose:
    This script performs quality checks to validate the integrity, consistency,
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.

================================================================================
*/

-- ================================================================================
-- Checking 'gold.dim_customers'
-- ================================================================================

-- Check for uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ================================================================================
-- Checking 'gold.dim_products'
-- ================================================================================

-- Check for uniqueness of Product Key in gold.dim_products
-- Expectation: No results
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ================================================================================
-- Checking 'gold.fact_sales'
-- ================================================================================

-- Check the data model connectivity between fact and dimensions
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
    ON f.product_key = p.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;

-- ================================================================================
-- Additional Data Quality Checks
-- ================================================================================

-- Check for NULL surrogate keys in fact_sales
-- Expectation: No results
SELECT
    COUNT(*) AS null_key_count
FROM gold.fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL;

-- Check for orphaned records in fact_sales (customers)
-- Expectation: No results
SELECT
    COUNT(*) AS orphaned_customer_records
FROM gold.fact_sales f
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.dim_customers c
    WHERE f.customer_key = c.customer_key
);

-- Check for orphaned records in fact_sales (products)
-- Expectation: No results
SELECT
    COUNT(*) AS orphaned_product_records
FROM gold.fact_sales f
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.dim_products p
    WHERE f.product_key = p.product_key
);

-- Validate sales amount calculation (sales_amount = quantity * price)
-- Expectation: No results
SELECT
    order_number,
    sales_amount,
    quantity,
    price,
    (quantity * price) AS calculated_amount
FROM gold.fact_sales
WHERE sales_amount != (quantity * price)
   OR sales_amount IS NULL
   OR quantity IS NULL
   OR price IS NULL;

-- Check for negative or zero values in critical measures
-- Expectation: No results
SELECT
    COUNT(*) AS invalid_measure_count
FROM gold.fact_sales
WHERE sales_amount <= 0
   OR quantity <= 0
   OR price <= 0;

-- Summary statistics for fact_sales
SELECT
    'gold.fact_sales' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT order_number) AS unique_orders,
    COUNT(DISTINCT customer_key) AS unique_customers,
    COUNT(DISTINCT product_key) AS unique_products,
    MIN(order_date) AS earliest_order_date,
    MAX(order_date) AS latest_order_date,
    SUM(sales_amount) AS total_revenue,
    AVG(sales_amount) AS avg_order_value
FROM gold.fact_sales;

-- Summary statistics for dim_customers
SELECT
    'gold.dim_customers' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_key) AS unique_customer_keys,
    COUNT(DISTINCT customer_id) AS unique_customer_ids,
    COUNT(DISTINCT country) AS unique_countries
FROM gold.dim_customers;

-- Summary statistics for dim_products
SELECT
    'gold.dim_products' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT product_key) AS unique_product_keys,
    COUNT(DISTINCT product_id) AS unique_product_ids,
    COUNT(DISTINCT category) AS unique_categories,
    COUNT(DISTINCT subcategory) AS unique_subcategories
FROM gold.dim_products;

-- ================================================================================
-- Date Integrity Checks
-- ================================================================================

-- Check for invalid date sequences in fact_sales
-- Expectation: No results
SELECT
    order_number,
    order_date,
    shipping_date,
    due_date
FROM gold.fact_sales
WHERE shipping_date < order_date
   OR due_date < order_date
   OR due_date < shipping_date;

-- Check for future dates in fact_sales
-- Expectation: No results (unless future orders are valid)
SELECT
    COUNT(*) AS future_order_count
FROM gold.fact_sales
WHERE order_date > GETDATE();

-- ================================================================================
-- Completeness Checks
-- ================================================================================

-- Check for NULL values in critical dimension fields (dim_customers)
SELECT
    'customer_id' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_customers
WHERE customer_id IS NULL
UNION ALL
SELECT
    'customer_number' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_customers
WHERE customer_number IS NULL
UNION ALL
SELECT
    'first_name' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_customers
WHERE first_name IS NULL
UNION ALL
SELECT
    'last_name' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_customers
WHERE last_name IS NULL;

-- Check for NULL values in critical dimension fields (dim_products)
SELECT
    'product_id' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_products
WHERE product_id IS NULL
UNION ALL
SELECT
    'product_number' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_products
WHERE product_number IS NULL
UNION ALL
SELECT
    'product_name' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_products
WHERE product_name IS NULL
UNION ALL
SELECT
    'category' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_products
WHERE category IS NULL;

PRINT 'Gold layer quality checks completed!';
GO
