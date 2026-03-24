/*
===========================================================================
Store PROCEDURE: Load Bronze Layer (Source -> Bronze)
===========================================================================
Script Purpose:
	This stored procedure loads data into bronze schema from external CSV files.
	Perform action:
		1) TRUNCATE the bronze table before loading data.
		2) Uses BULK INSERT to load data from CSV files to bronze tables.

	Parameter:
		None

	USAGE Example:
		EXEC bronze.load_bronze
===========================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	-- Declare parameter for collecting time to calculate process duration
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
		PRINT '====================';
		PRINT 'LOADING Bronze layer';
		PRINT '====================';

		PRINT '------------------';
		PRINT 'LOADING CRM tables';
		PRINT '------------------';
		-- Collect the time into parameter
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		PRINT 'TRUNCATING table bronze.crm_cust_info';
		-- Truncate the table before insert to provent duplicate insert
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'INSERTING table bronze.crm_cust_info';
		-- Insert data from file using bulk insert command
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\sorawit\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load duration: ' + CAST(DATEDIFF (second,@start_time,@end_time) AS NVARCHAR) + ' sec';

		-- table bronze.crm_prd_info
		SET @start_time = GETDATE()
		PRINT CHAR(10) + 'TRUNCATING table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'INSERTING table bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\sorawit\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load duration: ' + CAST(DATEDIFF (second,@start_time,@end_time) AS NVARCHAR) + ' sec';

		-- table bronze.crm_sales_details
		SET @start_time = GETDATE()
		PRINT CHAR(10) + 'TRUNCATING table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'INSERTING table bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\sorawit\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load duration: ' + CAST(DATEDIFF (second,@start_time,@end_time) AS NVARCHAR) + ' sec';

		PRINT CHAR(10) + '------------------'
		PRINT 'LOADING ERP tables'
		PRINT '------------------'
		-- table bronze.erp_cust_az12
		SET @start_time = GETDATE()
		PRINT 'TRUNCATING table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT 'INSERTING table bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\sorawit\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load duration: ' + CAST(DATEDIFF (second,@start_time,@end_time) AS NVARCHAR) + ' sec';

		-- table bronze.erp_loc_a101
		SET @start_time = GETDATE()
		PRINT CHAR(10) + 'TRUNCATING table bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT 'INSERTING table bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\sorawit\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load duration: ' + CAST(DATEDIFF (second,@start_time,@end_time) AS NVARCHAR) + ' sec';

		-- table bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE()
		PRINT CHAR(10) + 'TRUNCATING table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'INSERTING table bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\sorawit\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load duration: ' + CAST(DATEDIFF (second,@start_time,@end_time) AS NVARCHAR) + ' sec';
		SET @batch_end_time = GETDATE()

		PRINT '====================';
		PRINT 'LOADING SUCCESSFULLY';
		PRINT 'BATCH DURATION: ' + CAST(DATEDIFF (second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' sec';
		PRINT '====================';
	END TRY
	BEGIN CATCH
		PRINT '====================';
		PRINT 'ERROR DURING LOADING';
		PRINT 'BRONZE LAYER';
		PRINT ERROR_MESSAGE();
		PRINT '====================';
	END CATCH
END

-- EXEC bronze.load_bronze;
