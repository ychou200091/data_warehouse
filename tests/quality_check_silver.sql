/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

/*
=========================================
Quality Check for crm_cust_info
=========================================
*/
/*
- check duplicates
- check null columns
*/
/*
SELECT cst_id, count(*)
FROM silver.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) >1 OR cst_id is NULL


-- check and remove unwanted spaces.
-- expectation: no results
Select cst_firstname, cst_lastname
FROM silver.crm_cust_info
Where cst_firstname != TRIM(cst_firstname) or cst_lastname != TRIM(cst_lastname);

-- check for data consistency for cst_gndr and cst_marital_status 
-- crm_cust_info: Male, Female, Unknown
Select DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- cst_marital_status: Single, Married, Unknown
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;
*/

/*
=========================================
Quality Check for crm_prd_info
=========================================
*/

-- check for duplicated id
-- exp: no result
SELECT prd_id, count(*) as amount
FROM bronze.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) >1 OR prd_id is NULL;

-- check and remove unwanted spaces.
-- expectation: no results
Select prd_nm
FROM bronze.crm_prd_info
Where prd_nm != TRIM(prd_nm) ;

-- check for NULL and negative numbers
-- expectation: no results
Select prd_cost
FROM silver.crm_prd_info
Where prd_cost IS NULL or prd_cost<0;

-- Data standardization and Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check Dates, end date can't be earlier than start date.
-- end date can be null
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Overall look at  silver.crm_prd_info
SELECT * 
FROM silver.crm_prd_info


/* ==============================
Checking crm_sales_details
=================================
*/

-- overall view
SELECT 
	sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, 
	sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details

-- check if any sales prd_key is not in crm_prd_info table
-- check if any sales cust_in is not in crm_cust_info
SELECT 
	sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, 
	sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details as s
WHERE s.sls_prd_key NOT IN (SELECT p.prd_key FROM silver.crm_prd_info as p)
OR s.sls_cust_id NOT IN (SELECT c.cst_id FROM silver.crm_cust_info as c);
	
-- when executing the above query, it takes a long time.
-- Gemini's suggestion to the efficiency problem.
SELECT
    s.sls_ord_num, s.sls_prd_key, s.sls_cust_id, s.sls_order_dt,
    s.sls_ship_dt, s.sls_due_dt, s.sls_sales, s.sls_quantity, s.sls_price
FROM
    silver.crm_sales_details AS s
LEFT JOIN
    silver.crm_prd_info AS p ON s.sls_prd_key = p.prd_key
LEFT JOIN
    silver.crm_cust_info AS c ON s.sls_cust_id = c.cst_id
WHERE
    p.prd_key IS NULL OR c.cst_id IS NULL;

-- or just run one of the query one by one.
SELECT 
	sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, 
	sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details as s
WHERE s.sls_cust_id NOT IN (SELECT c.cst_id FROM silver.crm_cust_info as c);

SELECT 
	sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, 
	sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details as s 
WHERE s.sls_prd_key NOT IN (SELECT p.prd_key FROM silver.crm_prd_info as p);

-- Check for invalid dates on sls_order_dt, sls_ship_dt, sls_due_dt
SELECT NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE
	sls_order_dt <=0 OR
	LEN(sls_order_dt) != 8 OR
	sls_order_dt <= 19900101 OR
	sls_order_dt >= 20300101;

SELECT NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE
	sls_ship_dt <=0 OR
	LEN(sls_ship_dt) != 8 OR
	sls_ship_dt <= 19900101 OR
	sls_ship_dt >= 20300101;

SELECT NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE
	sls_due_dt <=0 OR
	LEN(sls_due_dt) != 8 OR
	sls_due_dt <= 19900101 OR
	sls_due_dt >= 20300101;

-- Check for invalid order dates
-- order_dt should be smaller than ship_dt and due date
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt

-- Check if sales, quantity, price numbers make sense.
-- sales = quantity * price
-- no null number and negative number
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales,sls_quantity,sls_price

/*
=======================================
=======================================
ERP
=======================================
=======================================

*/

/*
=======================================
erp_cust_az12
=======================================

*/
-- View overall table
SELECT cid,bdate,gen 
FROM bronze.erp_cust_az12;

-- See if  erp_cust_az12.cid can connect with crm_cust_info
SELECT * 
FROM bronze.erp_cust_az12
WHERE cid not in (SELECT cst_key FROM silver.crm_cust_info);
-- data wise, old records have 'NAS%' while newer data doesn't start with 'NAS', so remove all NAS
-- crm have newer records without 'NAS' 
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
	ELSE cid
	END cid,
	bdate,gen 
FROM silver.erp_cust_az12

-- check with CRM
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
	ELSE cid
	END cid,
	bdate,gen 
FROM silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
	ELSE cid
	END not in (SELECT cst_key FROM silver.crm_cust_info);


-- Find wired birthday (too old or in the future)
-- expectation: no result
SELECT cid,bdate,gen 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' or bdate > GETDATE()

-- Standardize Gender Column
-- Expectation: Male, Female, Unknown
SELECT Distinct gen 
FROM silver.erp_cust_az12


/*
===============================
erp_loc_a101
===============================
*/
-- Check for available cntry option
SELECT DISTINCT CNTRY
FROM silver.erp_loc_a101

SELECT *
FROM silver.erp_loc_a101


/*
===============================
erp_px_cat_g1v2
===============================
*/
select *
FROM silver.erp_px_cat_g1v2
