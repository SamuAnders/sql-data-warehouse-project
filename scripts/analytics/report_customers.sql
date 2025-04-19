/*
===============================================================================
CUSTOMER REPORTS
===============================================================================
PURPOSES:
    - This report consolidates key customer metrics and behaviors

HIGHLIGHTS:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/
IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS

/* 1) Base Query: Retrives core columns from tables */
WITH base_query AS (
SELECT 
sls.order_number, 
sls.product_key,
sls.order_date,
sls.sales_amount,
sls.quantity,
cu.customer_key,
cu.customer_number,
CONCAT(cu.first_name, ' ', cu.last_name) AS customer_name,
DATEDIFF(YEAR, cu.birthdate, GETDATE()) AS age
FROM gold.fact_sales sls
LEFT JOIN gold.dim_customers cu
ON sls.customer_key = cu.customer_key
WHERE order_date IS NOT NULL
)
, customer_aggregation AS (
-- 2) Customer Aggregations: Summarizes key metrics at the customer level
SELECT 
customer_key,
customer_number,
customer_name,
age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS quantity_sold,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order_date,
DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
FROM base_query
GROUP BY 
customer_key,
customer_number,
customer_name,
age)
-- 3. Final results: Adding calculates valuable KPIs including other criteria before
SELECT 
customer_key,
customer_number,
customer_name,
age,
CASE WHEN age < 20 THEN 'Under 20'
	WHEN age BETWEEN 20 AND 29 THEN '20-29'
	WHEN age BETWEEN 30 AND 39 THEN '30-39'
	WHEN age BETWEEN 40 AND 49 THEN '40-59'
	ELSE '50+ Above'
END AS age_group	,
CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
	WHEN lifespan >= 12 AND total_sales<= 5000 THEN 'Regular'
	ELSE 'New'
END AS customer_segment,
-- Recency
DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency,
total_orders,
total_sales,
-- Compute average order value (AVO)
CASE WHEN total_orders = 0 THEN 0
	ELSE total_sales/total_orders 
END AS avg_order_value,
-- Compute average monthly spend
CASE WHEN lifespan = 0 THEN 0
	ELSE total_sales/lifespan
END AS avg_monthly_spend,
quantity_sold,
total_products,
last_order_date,
lifespan
FROM customer_aggregation;