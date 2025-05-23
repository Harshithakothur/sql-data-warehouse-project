/*
============================================================================
DDL Script: Create Bronze Tables
============================================================================
Script Purpose:
	This scrpt creates tables in the 'bronze' schema, dropping existing tables
	if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
============================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info', 'U') is not null  --U is user defined
	DROP TABLE bronze.crm_cust_info

CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key nvarchar(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_create_date DATE
)

IF OBJECT_ID ('bronze.crm_prd_info', 'U') is not null  --U is user defined
	DROP TABLE bronze.crm_prd_info

CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_date DATETIME,
	prd_end_date DATETIME
)

IF OBJECT_ID ('bronze.crm_sales_details', 'U') is not null  --U is user defined
	DROP TABLE bronze.crm_sales_details

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_date INT,
	sls_ship_date INT,
	sls_due_date INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
)

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') is not null  --U is user defined
	DROP TABLE bronze.erp_loc_a101

CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') is not null  --U is user defined
	DROP TABLE bronze.erp_cust_az12

CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate date,
	gen NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') is not null  --U is user defined
	DROP TABLE bronze.erp_px_cat_g1v2

CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
)
