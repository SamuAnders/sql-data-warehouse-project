/*
==============================================================================
STORED PROCEDURE: A Full Load in Bronze Layer (source->bronze layer)
==============================================================================
Script Purpose:
	Loading the data from source (CSV) to our bronze schema.
	What the scripts do:
	- Truncate bronze table before loading data
	- Load the data using bulk insert

Parameters:
	None. 
	This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
*/
EXEC bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '-------------------------------------------------'
		PRINT 'Loading Bronze Layer'
		PRINT '-------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '=================================================='
		PRINT 'Begin Loading CRM Tables'
		PRINT '=================================================='
		
		PRINT 'Begin to truncate and bulk insert cust_info.csv'
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Programming Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT 'Begin to truncate and bulk insert prd_info.csv'
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Programming Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT 'Begin to truncate and bulk insert sales_details.csv'
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Programming Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '=================================================='
		PRINT 'Begin Loading ERP Tables'
		PRINT '=================================================='
		PRINT 'Begin to truncate and bulk insert cust_az12.csv'
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Programming Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT 'Begin to truncate and bulk insert loc_a101.csv'
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Programming Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT 'Begin to truncate and bulk insert px_cat_g1v2.csv'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Programming Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------'
		
		SET @batch_end_time = GETDATE()
		PRINT '>> Load bronze layer is COMPLETE, total duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
