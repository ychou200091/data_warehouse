/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
-- checking crm_cust_info.cst_id linking other tables' status
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.CID

-- Checking if duplicated id exist in the system

SELECT cst_id , COUNT(*) FROM (
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.CID
) t GROUP BY cst_id
HAVING COUNT(*) > 1;

-- there are 2 gender info in this system: cst_gndr, ca.gen
-- we need to do "data integration"
-- error case: 2 table gender mismatch.
-- handle method: CRM is the master table, if gender info not in crm, then use erp, else 'Unknown'
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr IN ( 'Male','Female') THEN ci.cst_gndr
		WHEN ci.cst_gndr = 'Unknown' AND ca.gen IN ('Male','Female') THEN ca.gen
	ELSE 'Unknown'
	END gender

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.CID


-- check genders of gold table dim_customers
SELECT DISTINCT gender  FROM gold.dim_customers

-- checking if duplicated product info exist after joining tables

SELECT prd_id, COUNT(*) FROM (
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
) t GROUP BY prd_id HAVING COUNT(*) >1;

-- checking if gold.dim_customers query works fine
SELECT * FROM gold.dim_customers
-- checking if gold.dim_product query works fine
SELECT * FROM gold.dim_product;
-- checking if gold.fact_sales query works fine
select * from gold.fact_sales;

-- Foreign Key Integrity Check (Dimension)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_product p 
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL OR p.product_key is NULL;
