-- Checking for NULL or Duplicates in Primary Key
-- Expectation:	No Result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR COUNT(prd_id) > 1;

-- Checking for Unwanted Spaces
-- Expectation: Clear, No Result
SELECT 
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Checking for negative number
-- Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Checking for Data Standardization & Consistency
SELECT DISTINCT 
maintenance
FROM bronze.erp_px_cat_g1v2;

-- See all data from silver.crm_cust_info
SELECT *
FROM silver.crm_cust_info;

-- Check for Invalid Date Orders
-- Expectation: No Result
SELECT TOP 5 *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT 
NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE 
sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101;

-- Checking for Invalid Order Dates
-- Expectation: No Result
SELECT
*
FROM silver.crm_sales_details
WHERE
sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt;

-- Identify Out of Range Dates
-- Expectation: No Result
SELECT DISTINCT
*
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR DATEADD(YEAR, 0, bdate) >= DATEADD(YEAR, -12, GETDATE());


-- Checking Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
SELECT DISTINCT
	sls_sales, 
	sls_quantity, 
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT DISTINCT
	sls_sales AS old_sls_sales,
	CASE WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL OR sls_sales <=0
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,  
	sls_price AS old_sls_price,
	CASE WHEN sls_price IS NULL OR sls_price <=0
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
FROM silver.crm_sales_details;

SELECT * FROM silver.erp_px_cat_g1v2;

-- Checking for changing in columns
SELECT        
	cid AS old_cid, 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	bdate, 
	gen
FROM silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);