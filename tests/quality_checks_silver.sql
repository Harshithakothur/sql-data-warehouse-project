/* 
==================================================================================
Quality Checks
==================================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy,
	and standardization across the 'silver' schemas. It includes checks for:
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields.
	- Data standardization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fields.

Usage Notes:
	- Run these checks after data loading silver layer.
	- Investigate and resolve any discrepancies found during the checks.
==================================================================================
*/

-- ================================================================================
-- Checking 'silver.crm_cust_info'
-- ================================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL

--Check for unwanted Spaces
-- Expectation: No Result
SELECT cst_lastname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Data Standardization & Consistency
SELECT DISTINCT cst_gender
from silver.crm_cust_info  

-- ===================================================================================
-- Checking 'silver.crm_prd_info'
-- ===================================================================================
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm  != TRIM(prd_nm )

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM [silver].[crm_prd_info]

--Check for Invalid Date Orders (start date > end date)
SELECT *  
FROM silver.crm_prd_info
WHERE prd_end_date < prd_start_date  

-- ==================================================================================
-- Checking 'silver.crm_sales_details'
-- ==================================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
NULLIF(sls_order_date,0) sls_order_date
FROM silver.crm_sales_details
WHERE sls_order_date <= 0 
OR LEN(sls_order_date) != 8 
OR sls_order_date > 20500101
OR sls_order_date < 19000101

--Check for Invalid Date Orders (order date > shipping/ due dates)
-- Expectation: No Result
SELECT *
FROM silver.crm_sales_details 
WHERE sls_order_date > sls_ship_date OR sls_order_date > sls_due_date

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero, or Negative
SELECT 
 sls_sales,
 sls_quantity,
 sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL 
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY old_sls_sales, sls_quantity, old_sls_price

-- ======================================================================================
-- Checking 'silver.erp_cust_az12'
-- ======================================================================================
-- Identify Out-of-Range Dates
-- EXpectation: Birthdates between the oldest date and today
select bdate
from  silver.erp_cust_az12
where bdate < '1924-01-01'  OR bdate > GETDATE()

--Data Standardization & Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12

-- ========================================================================================
-- Checking 'Silver.erp_loc_a101'
-- ========================================================================================
-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

-- =========================================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- =========================================================================================
--Check for Unwanted Spaces
-- Expectation: No Results
SELECT *
FROM [silver].[erp_px_cat_g1v2]
WHERE cat != TRIM(cat) 
OR subcat != TRIM(subcat)
OR maintenance != TRIM(maintenance )

-- Data Standardization & Consistency
SELECT DISTINCT maintenance 
FROM silver.erp_px_cat_g1v2
