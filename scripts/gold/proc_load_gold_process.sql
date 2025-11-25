-- join 3 tables: crm_cust_info,erp_cust_az12,erp_loc_a101
-- artifically created a key for this table "customer_key"
-- Filter column to only have 1 gender
-- rename columns

SELECT 
	ROW_NUMBER() OVER ( ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr IN ( 'Male','Female') THEN ci.cst_gndr
		WHEN ci.cst_gndr = 'Unknown' AND ca.gen IN ('Male','Female') THEN ca.gen
	ELSE 'Unknown'
	END gender,
	ca.bdate AS birthday,
	ci.cst_create_date AS create_date
	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.CID


/*
Product Table

*/

SELECT 
	pn.prd_id,
	pn.category_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance 
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.category_id = pc.id
WHERE prd_end_dt is NULL -- only want current products' data


/*

Product Table

-- rename columns
-- only want current products' data
-- this table is full of product descriptions so it is a dimension table
*/


SELECT 
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.category_id ,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance ,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt	AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.category_id = pc.id
WHERE prd_end_dt is NULL -- only want current products' data

