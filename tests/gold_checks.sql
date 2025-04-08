/*
==============================================================
GOLD LAYER QUALITY CHECKS
==============================================================
SCRIPT PURPOSES:
This scripts perform checks for duplicates, null, and foreign 
key integrity

USAGE NOTES:
- You can change it according to what you want to test
==============================================================
*/
-- Checking for Duplicates
SELECT cst_id, COUNT(*) FROM 
(
	SELECT        
		ci.cst_key, 
		ci.cst_id, 
		ci.cst_lastname, 
		ci.cst_firstname, 
		ci.cst_marital_status, 
		ci.cst_gndr, 
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid)t
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Get new gender info column based on master table if it is available. 
SELECT DISTINCT      
	ci.cst_gndr, 
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is The Master for Gender Info
		ELSE COALESCE(ca.gen, 'n/a') -- replace null with n/a
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1, 2;

-- Checking quality of gold tables
SELECT *
FROM gold.dim_customers;

SELECT * FROM gold.dim_products ORDER BY product_number;

SELECT * FROM gold.fact_sales;

-- Foreign Key Integrity (Dimensions)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
OR p.product_key IS NULL;
