/*
==============================================================================
EXPLORATORY DATA ANALYSIS: USING GOLD LAYER
==============================================================================
SCRIPT PURPOSES:
This script performs EDA in the gold layer. 
	Action Performed:
	1. Exploring database (Exploring all objects and columns in databases)
	2. Exploring dimension 
		- Identifying unique values or categories in each dimension
		- Analysing how data might be grouped or segmented
	3. Exploring Date 
		- Identifying the earliest and latest dates (boundaries) to understand scope of data and timespan
	4. Exploring measures 
		- Finding the key metrics (Big Number) of the business
	5. Magnitude Analysis
		- Compare measures value by categories and dimension to help understand the importance of different categories
	6. Ranking Analysis
		- Order/Rank the values of dimensions by value

PARAMETERS:
None. This procedure doesn't receive or return any values.

USAGE EXAMPLES:
Execute the specific block to test and analyze different component
==============================================================================
*/

-- 1. Database Exploration
-- Explore All Objects in the Database
SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- Explore All Columns in the Database
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME ='dim_products';

-- 2. Dimension Exploration
-- Explore All Country that Customer Come From
SELECT DISTINCT country 
FROM gold.dim_customers;

-- Explore All Categories 'The Major Divisions'
SELECT DISTINCT product_categories, subcategories, product_name
FROM gold.dim_products 
ORDER BY 1,2,3;

-- 3. Date Exploration
-- Find the date of the first and last order
SELECT MIN(order_date) AS first_order,
MAX(order_date) AS last_order, 
DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS month_length_order
FROM gold.fact_sales;

-- Find the youngest and oldest customer
SELECT MIN(birthdate) AS oldest_customer,
DATEDIFF(YEAR, MIN(birthdate), GETDATE()) AS oldest_age,
MAX(birthdate) AS youngest_customer,
DATEDIFF(YEAR, MAX(birthdate), GETDATE()) AS youngest_age
FROM gold.dim_customers;


-- 4. Measures Exploration
-- Find total sales
SELECT SUM(sales_amount) AS total_sales
FROM gold.fact_sales;

-- Find total product sold
SELECT SUM(quantity) AS total_quantity
FROM gold.fact_sales;

SELECT DISTINCT pro.product_name,
COUNT(sa.quantity) AS quantity_sold
FROM gold.fact_sales sa
LEFT JOIN gold.dim_products pro
	on sa.product_key = pro.product_key
GROUP BY pro.product_name
ORDER BY pro.product_name;

-- Find average selling price
SELECT AVG(price) as average_price
FROM gold.fact_sales;

-- Find total number of order
SELECT COUNT(DISTINCT order_number) as total_orders
FROM gold.fact_sales;

-- Find total number of products
SELECT COUNT(product_key) as total_products
FROM gold.dim_products;
SELECT COUNT(DISTINCT product_key) as total_products
FROM gold.dim_products;

-- Find total number of customers
SELECT COUNT(customer_key) FROM gold.dim_customers;

-- Find total number of customers that has placed an order
SELECT COUNT(DISTINCT customer_key) FROM gold.fact_sales;

-- Generate a report that shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, 
SUM(sales_amount) AS measure_value
FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity',
SUM(quantity)
FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price)
FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT order_number) as total_orders
FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(product_key) as total_products
FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(customer_key) FROM gold.dim_customers
UNION ALL
SELECT 'Total Nr. Customers Have Ordered', COUNT(DISTINCT customer_key) FROM gold.fact_sales;

-- 5. Magnitude Analysis
-- Find total sales by countries
SELECT cu.country, 
SUM(sls.sales_amount) AS total_sales
FROM gold.fact_sales sls
LEFT JOIN gold.dim_customers cu
	ON sls.customer_key = cu.customer_key
GROUP BY cu.country
ORDER BY total_sales DESC;

-- Find total customers by countries
SELECT country,
COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country;

-- Find total customers by gender
SELECT gender,
COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender;

-- Find total products by category
SELECT product_categories,
COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY product_categories
ORDER BY product_categories DESC;

-- What is the average costs in each category?
SELECT product_categories, 
AVG(cost) AS avg_product_cost
FROM gold.dim_products
GROUP BY product_categories
ORDER BY avg_product_cost DESC;

-- What is the total revenue generated for each category?
SELECT pro.product_categories,
SUM(sls.sales_amount) AS total_revenue
FROM gold.fact_sales sls
LEFT JOIN gold.dim_products pro
	ON sls.product_key = pro.product_key
GROUP BY pro.product_categories
ORDER BY total_revenue DESC;

-- Find total revenue is generated by each customer
SELECT cu.customer_key,
cu.first_name,
cu.last_name,
SUM(sls.sales_amount) AS total_spend
FROM gold.fact_sales sls
LEFT JOIN gold.dim_customers cu
	ON sls.customer_key = cu.customer_key
GROUP BY 
cu.customer_key,
cu.first_name,
cu.last_name
ORDER BY total_spend DESC;

-- What is the distribution of sold items accross countries?
SELECT
cu.country,
SUM(sls.quantity) AS sold_items
FROM gold.fact_sales sls
LEFT JOIN gold.dim_customers cu
	ON sls.customer_key = cu.customer_key
GROUP BY 
cu.country
ORDER BY sold_items DESC;

-- 6. Ranking Analysis
-- Which 5 products generate the highest revenue?
SELECT TOP 5
pro.product_name,
SUM(sls.sales_amount) AS total_revenue
FROM gold.fact_sales sls
LEFT JOIN gold.dim_products pro
	ON sls.product_key = pro.product_key
GROUP BY pro.product_name
ORDER BY total_revenue DESC;]

-- with window function
SELECT *
FROM (
	SELECT
	ROW_NUMBER() OVER(ORDER BY SUM(sls.sales_amount) DESC) AS rank_products,
	pro.product_name,
	SUM(sls.sales_amount) AS total_revenue
	FROM gold.fact_sales sls
	LEFT JOIN gold.dim_products pro
		ON sls.product_key = pro.product_key
	GROUP BY pro.product_name)t
WHERE rank_products <=5;

-- Which 5 products generate the lowest revenue?
SELECT TOP 5
pro.product_name,
SUM(sls.sales_amount) AS total_revenue
FROM gold.fact_sales sls
LEFT JOIN gold.dim_products pro
	ON sls.product_key = pro.product_key
GROUP BY pro.product_name
ORDER BY total_revenue ASC;

-- Find Top 10 Customers who have generated the highest revenue
SELECT TOP 10
cu.customer_key,
cu.first_name,
cu.last_name,
SUM(sls.sales_amount) AS total_revenue
FROM gold.fact_sales sls
LEFT JOIN gold.dim_customers cu
	ON sls.customer_key = cu.customer_key
GROUP BY
cu.customer_key,
cu.first_name,
cu.last_name
ORDER BY total_revenue DESC;

-- 3 customers with the fewest orders placed
SELECT TOP 3
cu.customer_key,
cu.first_name,
cu.last_name,
COUNT(DISTINCT sls.order_number) AS total_orders
FROM gold.fact_sales sls
LEFT JOIN gold.dim_customers cu
	ON sls.customer_key = cu.customer_key
GROUP BY
cu.customer_key,
cu.first_name,
cu.last_name
ORDER BY total_orders ASC;
