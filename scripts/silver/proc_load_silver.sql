/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT 'Loading Silver Layer';
		PRINT '================================================';
		/*
		Take bronze.crm_cust_info and clean it to put it into silver.crm_cust_info

		Operations include: 
		-- ranking the duplicates by cst_create_date and pick out only the newest record 
		-- remove spaces from firstname and lastname
		-- cst_gndr: F, M, NULL, convert to: Female, Male, Unknown
		-- cst_marital_status: S, M, NULL, convert to: Single, Married, Unknown

		*/
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting table: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
		cst_id,cst_key,cst_firstname,cst_lastname,
		cst_marital_status,cst_gndr,cst_create_date
		)
		SELECT 
			cst_id, 
			cst_key, 
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE WHEN UPPER(cst_marital_status) = 'S' Then 'Single'
				 WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
				 ELSE 'Unknown'
			END cst_marital_status,
			CASE	WHEN UPPER(cst_gndr) = 'F' THEN 'Female' 
					WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
					ELSE 'Unknown'
			END cst_gndr,
			cst_create_date
		FROM (
		SELECT *,ROW_NUMBER() Over(Partition by cst_id ORDER BY cst_create_date DESC ) as flag_order
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		) as t Where flag_order = 1

		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST( Datediff (SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';


		/*
		Take bronze.crm_prd_info and clean it to put it into silver.crm_prd_info

		Operations include: 
		-- extract category_id from prd_key,
		-- extract rest of the prd_key
		-- replace null prd_cost with 0
		-- standardize prd line abbreivations: NULL, M,R,S,T -> Unknown, Road, Other sales, Touring.
		-- take the start date of the next record as the end date of the previous record.
		*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting table: silver.crm_prd_info'
		INSERT silver.crm_prd_info (
		prd_id, category_id, prd_key, prd_nm, 
		prd_cost,prd_line, prd_start_dt, prd_end_dt
		)
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

		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST( Datediff (SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';


		/* 

		Take bronze.crm_sales_details and clean it to put it into silver.crm_sales_details


		-- For sls_order_dt, sls_ship_dt, sls_due_dt,
			Convert invalid dates to NULL, and valid dates to DATE format

		-- Check if sales, quantity, price numbers make sense.
			-- sales = quantity * price
			-- no null number and negative number
			-- 1. when sales is negative, zero, or null, derive it using quantity and price.
			-- 2. when price is null or zero, calculate it using sales and quantity.
			-- 3. when price is negative, convert it to positive
		*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting table: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
		sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, 
		sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
		)
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
		
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST( Datediff (SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';


		/*
		Take bronze.erp_cust_az12 and clean it to put it into silver.erp_cust_az12
 
		-- remove old records' 'NAS%' beginning
		-- make future birthday null
		-- Standardize Gender Column: Male, Female, Unknown
		*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting table: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
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

		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST( Datediff (SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';


		/*
		Take bronze.erp_loc_a101 and clean it to put it into silver.erp_loc_a101
 
		-- remove the '-' in cid inorder to match with erp_cust_az12 cid key. 
		-- Alter and remove duplicates, sync same to have unified country names
			'USA', 'US', 'United States' ---> 'United States'
			'DE' ---> 'Germany'
			etc.
		*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting table: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT REPLACE(cid, '-',''),  
			CASE 
			WHEN TRIM(cntry) IN ('USA', 'US', 'United States') THEN 'United States'
			WHEN TRIM(cntry) IN ('UK', 'United Kingdom') THEN 'United Kingdom'
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) ='' OR TRIM(cntry) IS NULL THEN 'Unknown'
			ELSE TRIM(cntry)
			END as CNTRY
		FROM bronze.erp_loc_a101
		
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST( Datediff (SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		/*
		Take bronze.erp_px_cat_g1v2 and clean it to put it into silver.erp_px_cat_g1v2
		-- nothing to be change
		*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting table: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT id,cat,subcat,maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST( Datediff (SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @batch_end_time = GETDATE();
		PRINT '================================';
		PRINT 'Loading Silver Layer is Complete';
		PRINT '		- Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================';

	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR Message '+ ERROR_MESSAGE();
		PRINT 'ERROR Message ' + CAST( ERROR_NUMBER() AS VARCHAR) ;
		PRINT 'ERROR Message ' + CAST( ERROR_STATE() AS VARCHAR) ;
	END CATCH
	PRINT '================================================';
END
