/*
ROW_NUMBER() : Assign a row number to each row based on a defined order.
*/

/*
=======================
crm_cust_info
=======================
*/

-- just checking the records of one of the duplicated ids 29466
SELECT *
FROM bronze.crm_cust_info
Where cst_id = 29466; 

-- ranking the duplicates by cst_create_date
-- differ from group by, you can see the records with the same cst_id.
SELECT *,ROW_NUMBER() Over(Partition by cst_id ORDER BY cst_create_date DESC ) as flag_order
FROM bronze.crm_cust_info
Where cst_id = 29466;
-- pick out only the newest record
-- pick out duplicated older records
Select *
FROM (
SELECT *,ROW_NUMBER() Over(Partition by cst_id ORDER BY cst_create_date DESC ) as flag_order
FROM bronze.crm_cust_info
) as t
-- WHERE flag_order != 1;
WHERE flag_order = 1;


-- next
-- check and remove unwanted spaces.
-- expectation: no results
Select cst_firstname, cst_lastname
FROM bronze.crm_cust_info
Where cst_firstname != TRIM(cst_firstname) or cst_lastname != TRIM(cst_lastname);



-- remove spaces
Select 
	cst_id, 
	cst_key, 
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
FROM bronze.crm_cust_info
Where cst_firstname != TRIM(cst_firstname) or cst_lastname != TRIM(cst_lastname);


-- check for data consistency for cst_gndr and cst_marital_status 
-- cst_gndr: F, M, NULL
-- convert to: Female, Male, Unknown
Select DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- cst_marital_status: S, M, NULL
-- convert to: Single, Married, Unknown
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

-- converting abbriviation to understandable nouns.
SELECT 
	cst_id, 
	cst_key, 
	cst_firstname,
	cst_lastname,
	CASE WHEN UPPER(cst_marital_status) = 'S' Then 'Single'
		 WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
		 ELSE 'Unknown'
	END cst_marital_status,
	CASE	WHEN UPPER(cst_gndr) = 'F' THEN 'Female' 
			WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
			ELSE 'Unknown'
	END cst_gndr,
	
	cst_create_date
FROM bronze.crm_cust_info



-- ===================================
-- bronze.crm_prd_info
-- ===================================

-- extract category_key from prd_key,
-- for category id, replace '-' with '_' because joining with bronze.erp_px_cat_g1v2 require this.
SELECT 
prd_id,prd_key,
Replace( SUBSTRING(prd_key, 1,5), '-','_') AS category_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;


-- Check if any extracted id is not in bronze.erp_px_cat_g1v2, where we are joining.
SELECT 
prd_id,prd_key,
Replace( SUBSTRING(prd_key, 1,5), '-','_') AS category_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE Replace( SUBSTRING(prd_key, 1,5), '-','_') NOT IN  (
SELECT Distinct id
FROM bronze.erp_px_cat_g1v2 )
 
-- extracted category id
-- also extract rest of the prd_key
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1,5), '-','_') AS category_id,
	SUBSTRING(prd_key, 7,len(prd_key) ) AS product_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;

-- check for NULL cost and replace with zero
Select ISNULL(prd_cost, 0) AS prd_cost
FROM bronze.crm_prd_info;

-- Data standardization and Consistency
-- prd_line: NULL, M,R,S,T 
	-- convert to: Unknown, Mountain, Road, Other sales, Touring
SELECT 
prd_id,
prd_key,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
	 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	 ELSE 'Unknown'
END AS prd_line
FROM bronze.crm_prd_info;
-- shorter way of writing the above query
SELECT 
prd_id,
prd_key,
CASE UPPER(TRIM(prd_line)) 
	When 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'Unknown'
END AS prd_line
FROM bronze.crm_prd_info;


-- Check Dates, end date can't be earlier than start date.
-- this query takes the start date of the next order(with same prd_key) to be the end date of the current order.
SELECT 
prd_id,
prd_key,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) as prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509','CL-JE-LJ-0192-S','CO-RF-FR-R92R-52');


/*
OVERALL for crm_prd_info, Operations include: 
-- extract category_id from prd_key,
-- extract rest of the prd_key
-- replace null prd_cost with 0
-- standardize prd line abbreivations: NULL, M,R,S,T -> Unknown, Road, Other sales, Touring.
-- take the start date of the next record as the end date of the previous record.
*/

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1,5), '-','_') AS category_id,
SUBSTRING(prd_key, 7,len(prd_key) ) AS prd_key,
prd_nm,	
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line)) 
	When 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'Unknown'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt ,
CAST( LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) as prd_end_dt
FROM bronze.crm_prd_info



/* 
=======================================
Transformation for crm_sales_details
=======================================
 */


-- For sls_order_dt, Convert invalid dates to NULL, and valid dates to DATE format
SELECT 
	sls_ord_num, sls_prd_key, sls_cust_id, 
	CASE WHEN sls_order_dt<=0 OR LEN(sls_order_dt)!=8 THEN NULL
		ELSE CAST(CAST( sls_order_dt AS VARCHAR)AS DATE)
	END sls_order_dt,
	sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details

-- For sls_order_dt, sls_ship_dt, sls_due_dt
-- Convert invalid dates to NULL, and valid dates to DATE format
SELECT 
	sls_ord_num, sls_prd_key, sls_cust_id, 
	CASE WHEN sls_order_dt<=0 OR LEN(sls_order_dt)!=8 THEN NULL
		ELSE CAST(CAST( sls_order_dt AS VARCHAR)AS DATE)
	END sls_order_dt,
	CASE WHEN sls_ship_dt<=0 OR LEN(sls_ship_dt)!=8 THEN NULL
		ELSE CAST(CAST( sls_ship_dt AS VARCHAR)AS DATE)
	END sls_ship_dt,
	CASE WHEN sls_due_dt<=0 OR LEN(sls_due_dt)!=8 THEN NULL
		ELSE CAST(CAST( sls_due_dt AS VARCHAR)AS DATE)
	END sls_due_dt,
	sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details


-- Check if sales, quantity, price numbers make sense.
-- sales = quantity * price
-- no null number and negative number
-- 1. when sales is negative, zero, or null, derive it using quantity and price.
-- 2. when price is null or zero, calculate it using sales and quantity.
-- 3. when price is negative, convert it to positive
SELECT 
sls_ord_num,sls_prd_key,
CASE WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END sls_sales,
CASE WHEN sls_price is NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
ELSE sls_price
END sls_price,sls_quantity
FROM bronze.crm_sales_details
ORDER BY sls_sales,sls_quantity,sls_price


/* overall operation to crm_sales_details

-- For sls_order_dt, sls_ship_dt, sls_due_dt
-- Convert invalid dates to NULL, and valid dates to DATE format

-- Check if sales, quantity, price numbers make sense.
-- sales = quantity * price
-- no null number and negative number
-- 1. when sales is negative, zero, or null, derive it using quantity and price.
-- 2. when price is null or zero, calculate it using sales and quantity.
-- 3. when price is negative, convert it to positive
*/

SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	CASE WHEN sls_order_dt<=0 OR LEN(sls_order_dt)!=8 THEN NULL
		ELSE CAST(CAST( sls_order_dt AS VARCHAR)AS DATE)
	END sls_order_dt,
	CASE WHEN sls_ship_dt<=0 OR LEN(sls_ship_dt)!=8 THEN NULL
		ELSE CAST(CAST( sls_ship_dt AS VARCHAR)AS DATE)
	END sls_ship_dt,
	CASE WHEN sls_due_dt<=0 OR LEN(sls_due_dt)!=8 THEN NULL
		ELSE CAST(CAST( sls_due_dt AS VARCHAR)AS DATE)
	END sls_due_dt,
	CASE WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales,
	sls_quantity, 
	CASE WHEN sls_price is NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
	END sls_price
FROM bronze.crm_sales_details
ORDER BY sls_cust_id,sls_price


/*
===============================
erp_cust_az12
===============================
*/

-- data wise, old records have 'NAS%' while newer data doesn't start with 'NAS', so remove all NAS
-- crm have newer records without 'NAS' 
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
	ELSE cid
	END cid,
	bdate,gen 
FROM bronze.erp_cust_az12

-- make future birthday null(too old or in the future)
SELECT cid,
CASE WHEN bdate>GETDATE() THEN NULL
	ELSE bdate
END bdate,
gen 
FROM bronze.erp_cust_az12

-- Standardize Gender Column
-- Expectation: Male, Female, Unknown
SELECT 
cid,
CASE WHEN UPPER(TRIM(gen)) in ('F','Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) in ('M','Memale') THEN 'Male'
	ELSE 'Unknown'
END gen
FROM bronze.erp_cust_az12

/*
overall for erp_cust_az12
-- remove old records' 'NAS%' beginning
-- make future birthday null
-- Standardize Gender Column: Male, Female, Unknown
*/

SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
	ELSE cid
	END cid,
	CASE WHEN bdate>GETDATE() THEN NULL
		ELSE bdate
	END bdate,
	CASE WHEN UPPER(TRIM(gen)) in ('F','Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) in ('M','Memale') THEN 'Male'
	ELSE 'Unknown'
	END gen
	
FROM bronze.erp_cust_az12

/*
========================================
erp_loc_a101
========================================
*/
-- remove the '-' in cid inorder to match with erp_cust_az12 cid key. 
-- for example, 'AW-00011000' becomes 'AW00011000'
SELECT REPLACE(cid, '-','')
FROM bronze.erp_loc_a101

-- Check for available cntry option
SELECT DISTINCT CNTRY
FROM bronze.erp_loc_a101
-- Alter and remove duplicates, sync same to have unified country names
SELECT DISTINCT CASE 
	WHEN TRIM(cntry) IN ('USA', 'US', 'United States') THEN 'United States'
	WHEN TRIM(cntry) IN ('UK', 'United Kingdom') THEN 'United Kingdom'
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) ='' OR TRIM(cntry) IS NULL THEN 'Unknown'
	ELSE TRIM(cntry)
	END as CNTRY
FROM  bronze.erp_loc_a101


/*
Overall operation on erp_loc_a101
-- remove the '-' in cid inorder to match with erp_cust_az12 cid key. 
-- Alter and remove duplicates, sync same to have unified country names
	'USA', 'US', 'United States' ---> 'United States'
	'DE' ---> 'Germany'
	etc.
*/
SELECT REPLACE(cid, '-',''),  
	CASE 
	WHEN TRIM(cntry) IN ('USA', 'US', 'United States') THEN 'United States'
	WHEN TRIM(cntry) IN ('UK', 'United Kingdom') THEN 'United Kingdom'
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) ='' OR TRIM(cntry) IS NULL THEN 'Unknown'
	ELSE TRIM(cntry)
	END as CNTRY
FROM bronze.erp_loc_a101


/*
===============================
erp_px_cat_g1v2
===============================
*/
select * 
from bronze.erp_px_cat_g1v2
-- Check for unwanted space
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance )
-- Data standardization and Consistency
select DISTINCT MAINTENANCE
from bronze.erp_px_cat_g1v2