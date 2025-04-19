/*
===============================================================================
ANALYTICS
===============================================================================
PURPOSES:
This script is used to do various analytics:
1. Change Over Time analysis
	- To explore the structure of the database, including the list of tables and their schemas.
	- To inspect the columns and metadata for specific tables.
2. Cumulative Analysis
	- To track cumulative performance
	- To analyze growth for long term goals
3. Performance Analysis
	- Measuring the performance of products, customers, or regions over time
	- Benchmarking and identifying high-performing entities
	- Tracking growth by comparing to previous sales
4. Part to Whole Analysis
	- To compare performance or metrics across dimensions or time periods.
    	- To evaluate differences between categories.
    	- Useful for A/B testing or regional comparisons. 
5. Data Segmentation
	- Group data to meaningful categories for targeted insights
	- For customer segmentation, product categorization, or regional analysis

SQL FUNCTIONS USED:
	- Date Functions: DATEPART(), DATETRUNC(), FORMAT()
    	- Aggregate Functions: SUM(), COUNT(), AVG()
	- Window Functions: SUM() OVER(), AVG() OVER()
	- LAG(): Accesses data from previous rows.
	- CASE: Defines conditional logic for trend analysis.
	- GROUP BY: Groups data into segments.
	- CTEs: A virtual table that only its main query can access
===============================================================================
*/
-- 1. Change Over Time analysis
-- Analyze Sales Peformance Overtime
SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME ='dim_products';

SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME ='dim_customers';

SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME ='fact_sales';

-------------------------------------------------------------
SELECT 
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL 
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

-- OR

SELECT 
DATETRUNC(MONTH,order_date) AS order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL 
GROUP BY DATETRUNC(MONTH,order_date)
ORDER BY DATETRUNC(MONTH,order_date);

-- OR
SELECT 
FORMAT(order_date, 'yyyy-MMM') AS order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL 
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM');
-------------------------------------------------------------

-- 2. Cumulative Analysis
-- Running Total Sales by Year
SELECT 
order_date,
total_sales,
average_price,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
AVG(average_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
SELECT DATETRUNC(MONTH,order_date) AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS average_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH,order_date)
)t;

-- 3. Performance Analysis
/* Analyze yearly performance by comparing their sales to average sales perfomance of the product and the previous year's sales */
-- CTEs
WITH yearly_product_sales AS (
SELECT 
YEAR(sls.order_date) AS order_year,
pro.product_name,
SUM(sls.sales_amount) AS current_sales
FROM gold.fact_sales sls
LEFT JOIN gold.dim_products pro
ON sls.product_key = pro.product_key
WHERE order_date IS NOT NULL
GROUP BY 
YEAR(sls.order_date),
pro.product_name)

SELECT 
order_year,
product_name,
current_sales,
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_previous_year,
CASE WHEN current_sales - current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
	WHEN current_sales - current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
	ELSE 'No change'
END previous_year_change,
AVG(current_sales) OVER (PARTITION BY product_name) avg_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Average'
	WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Average'
	ELSE'Average'
END avg_change
FROM yearly_product_sales
ORDER BY product_name, order_year;

-- 4. Part to Whole Analysis
/* Analyze how individual part is performing compared to the overall, allowing to undestand which category has the greatest impact on the business */
-- Which categories contribute the most to overall sales?
WITH category_sales AS (
SELECT 
pro.product_categories,
SUM(sls.sales_amount) AS total_sales
FROM gold.fact_sales sls
LEFT JOIN gold.dim_products pro
ON sls.product_key = pro.product_key
GROUP BY pro.product_categories
)
SELECT 
product_categories,
total_sales,
SUM(total_sales) OVER () overall_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ())*100, 3), '%') AS percentage_of_total -- avoid zero by casting total sales to float
FROM category_sales
ORDER BY total_sales DESC;

-- 5. Data Segmentation
/* Segment products into cost ranges and count how many products fall into each segment */
WITH product_segment AS (
SELECT 
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
	WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE 'Above 1000'
END cost_range
FROM gold.dim_products
)
SELECT 
cost_range,
COUNT(product_key) AS total_product
FROM product_segment
GROUP BY cost_range
ORDER BY total_product DESC;

/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/
WITH customer_spending AS (
SELECT 
cus.customer_key,
SUM(sls.sales_amount) AS total_spending,
MIN(sls.order_date) AS first_order,
MAX(sls.order_date) AS last_order,
DATEDIFF(MONTH, MIN(sls.order_date), MAX(sls.order_date)) AS lifespan
FROM gold.fact_sales sls
LEFT JOIN gold.dim_customers cus
ON sls.customer_key = cus.customer_key
GROUP BY cus.customer_key
)
SELECT
customer_segment,
COUNT(customer_key) AS total_customers
FROM (
SELECT 
customer_key,
CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
	WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
	ELSE 'New'
END customer_segment
FROM customer_spending)t
GROUP BY customer_segment;


