/*
============================================================================
DDL Script: Create Silver Tables
============================================================================
Script Purpose:
	This scrpt creates tables in the 'silver' schema, dropping existing tables
	if they already exist.
	Run this script to re-define the DDL structure of 'silver' Tables
============================================================================
*/

IF OBJECT_ID ('silver.crm_cust_info', 'U') is not null  --U is user defined
	DROP TABLE silver.crm_cust_info

CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_date DATE,
	prd_end_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.crm_prd_info', 'U') is not null  --U is user defined
	DROP TABLE silver.crm_prd_info

CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_date DATETIME,
	prd_end_date DATETIME,
	dwh_create_date DATETIME2 DEFAULT GETDATE() 
)

IF OBJECT_ID ('silver.crm_sales_details', 'U') is not null  --U is user defined
	DROP TABLE silver.crm_sales_details

CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_date DATE,
	sls_ship_date DATE,
	sls_due_date DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE() 
)

IF OBJECT_ID ('silver.erp_loc_a101', 'U') is not null  --U is user defined
	DROP TABLE silver.erp_loc_a101

CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE() 
)

IF OBJECT_ID ('silver.erp_cust_az12', 'U') is not null  --U is user defined
	DROP TABLE silver.erp_cust_az12

CREATE TABLE silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate date,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE() 
)

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') is not null  --U is user defined
	DROP TABLE silver.erp_px_cat_g1v2

CREATE TABLE silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE() 
)
