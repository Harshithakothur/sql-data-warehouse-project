/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
	None.
	This stored procedures does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze
==================================================================================
*/

CREATE or ALTER PROCEDURE bronze.load_bronze as
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		PRINT '================================================='
		PRINT 'Loading Bronze Layer'
		PRINT '================================================='

		PRINT '-------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '-------------------------------------------------'

		SET @batch_start_time = GETDATE()
		SET @start_time  = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_cust_info '
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\SQL_data_warehouse_project\sql-data-warehouse-project\dataSETs\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time  = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '-----------'

		SET @start_time  = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_prd_info  '
		TRUNCATE TABLE bronze.crm_prd_info 

		PRINT '>> Inserting Data Into: bronze.crm_prd_info '
		BULK INSERT  bronze.crm_prd_info
		FROM 'D:\SQL_data_warehouse_project\sql-data-warehouse-project\dataSETs\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time  = GETDATE()
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		PRINT '------------'

		SET @start_time  = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_sales_details '
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inserting Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\SQL_data_warehouse_project\sql-data-warehouse-project\dataSETs\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time  = GETDATE()
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		PRINT '------------'

		PRINT '------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '------------------------------------------'

		SET @start_time  = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_LOC_A101 '
		TRUNCATE TABLE bronze.erp_LOC_A101

		PRINT '>> Inserting Data Into: bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\SQL_data_warehouse_project\sql-data-warehouse-project\dataSETs\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time  = GETDATE()
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		PRINT '------------'

		SET @start_time  = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_CUST_AZ12 '
		TRUNCATE TABLE bronze.erp_CUST_AZ12

		PRINT '>> Inserting Data Into: bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\SQL_data_warehouse_project\sql-data-warehouse-project\dataSETs\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time  = GETDATE()
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		PRINT '------------'

		SET @start_time  = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2 '
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
		BULK INSERT  bronze.erp_px_cat_g1v2
		FROM 'D:\SQL_data_warehouse_project\sql-data-warehouse-project\dataSETs\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time  = GETDATE()
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		PRINT '------------'
		
		SET @batch_end_time = GETDATE()
		PRINT '-------------------------'
		PRINT '>> Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds'
		PRINT '-------------------------'
	END TRY
	BEGIN CATCH
		PRINT '==========================================================='
		PRINT 'error occurred during loading bronze layer'
		PRINT 'error message'+ error_message()
		PRINT 'error message' + cast(error_number() as nvarchar)
		PRINT 'error message' + cast(error_state() as nvarchar)
		PRINT '==========================================================='
	END CATCH
END
