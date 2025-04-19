/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
	DROP VIEW gold.report_products;
GO

CREATE VIEW gold.report_products AS
-- 1) Base Query: Retrives core columns from tables 
WITH base_query AS (
SELECT 
sls.order_number,
sls.order_date,
sls.customer_key,
sls.sales_amount,
sls.quantity,
pro.product_key,
pro.product_categories,
pro.subcategories,
pro.product_name,
pro.cost
FROM gold.fact_sales sls
LEFT JOIN gold.dim_products pro
ON sls.product_key = pro.product_key
), 
-- 2) Product Aggregations: Summarizes key metrics at product level
product_aggregations AS (
SELECT
product_key,
product_name,
product_categories,
subcategories,
cost,
COUNT(DISTINCT order_number) AS total_order,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT customer_key) AS total_customers,
DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
MAX(order_date) AS last_order_date,
ROUND(AVG(CAST(sales_amount AS FLOAT)/NULLIF(quantity, 0)),1) AS avg_selling_price
FROM base_query
GROUP BY 
product_key,
product_name,
product_categories,
subcategories,
cost
)
-- 3) Final Query: Combines all product results into one output
SELECT 
product_key,
product_name,
product_categories,
subcategories,
cost,
total_order,
last_order_date,
DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency_in_months,
CASE 
	WHEN total_sales > 50000 THEN 'High-Performer'
	WHEN total_sales >=10000 THEN 'Mid-Range'
	ELSE 'Low-Performer'
END AS product_segment,
total_sales,
total_quantity,
total_customers,
lifespan,
avg_selling_price,
-- Average Order Revenue (AOR)
CASE WHEN total_order = 0 THEN 0
	ELSE total_sales/total_order
END AS avg_order_revenue,
-- Average Monthly Revenue
CASE WHEN lifespan = 0 THEN 0
	ELSE total_sales/lifespan
END AS avg_monthly_revenue
FROM product_aggregations;