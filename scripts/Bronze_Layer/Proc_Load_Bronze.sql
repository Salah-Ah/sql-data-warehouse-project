/*
================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_batch DATETIME, @end_time_batch DATETIME;	
	BEGIN TRY
		SET @start_time_batch = GETDATE();
		PRINT '============================================'
		PRINT 'LOADING BRONZE LAYER'
		PRINT '============================================'
		PRINT '                                            '
	
		PRINT '============================================'
		PRINT 'LOADING CRM TABLES'
		PRINT '============================================'

		PRINT '                                            '
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: [bronze].[crm_cust_info]'
		TRUNCATE TABLE [bronze].[crm_cust_info]
		PRINT '>> INSERTING DATA INTO:[bronze].[crm_cust_info]'
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'D:\Data Analysis\Projects\4. Report_Readmes\SQL\Building DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOADING TABLE DURATION: ' + CAST( DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS'
		
		
		PRINT '                                            '
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: [bronze].[crm_prd_info]'
		TRUNCATE TABLE [bronze].[crm_prd_info]
		PRINT '>> INSERTING DATA INTO: [bronze].[crm_prd_info]'
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'D:\Data Analysis\Projects\4. Report_Readmes\SQL\Building DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOADING TABLE DURATION: ' + CAST( DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS'


		PRINT '                                            '
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: [bronze].[crm_sales_details]'
		TRUNCATE TABLE [bronze].[crm_sales_details]
		PRINT '>> INSERTING DATA INTO: [bronze].[crm_sales_details]'
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'D:\Data Analysis\Projects\4. Report_Readmes\SQL\Building DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOADING TABLE DURATION: ' + CAST( DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS'


		PRINT '                                            '
		PRINT '============================================'
		PRINT 'LOADING ERP TABLES'
		PRINT '============================================'

		
		PRINT '                                            '
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: [bronze].[erp_cust_az12]'
		TRUNCATE TABLE [bronze].[erp_cust_az12]
		PRINT '>> INSERTING DATA INTO: [bronze].[erp_cust_az12]'
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'D:\Data Analysis\Projects\4. Report_Readmes\SQL\Building DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOADING TABLE DURATION: ' + CAST( DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS'


		PRINT '                                            '
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: [bronze].[erp_loc_a101]'
		TRUNCATE TABLE [bronze].[erp_loc_a101]
		PRINT '>> INSERTING DATA INTO: [bronze].[erp_loc_a101]'
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'D:\Data Analysis\Projects\4. Report_Readmes\SQL\Building DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOADING TABLE DURATION: ' + CAST( DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS'


		PRINT '                                            '
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: [bronze].[erp_px_cat_g1v2]'
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]
		PRINT '>> INSERTING DATA INTO: [bronze].[erp_px_cat_g1v2]'
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'D:\Data Analysis\Projects\4. Report_Readmes\SQL\Building DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOADING TABLE DURATION: ' + CAST( DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR) + ' SECONDS'

		SET @end_time_batch = GETDATE();
		PRINT '>>> LOADING WHOLE BATCH DURATION: ' + CAST(DATEDIFF(SECOND,@start_time_batch,@end_time_batch) AS NVARCHAR) +  ' SECONDS';
	END TRY
	BEGIN CATCH
		PRINT '============================================';
		PRINT 'ERRORS OCCUERED DURING LOADING BRONZE LAYER';
		PRINT '============================================';
		PRINT 'ERROR MESSAGE:'+ ERROR_MESSAGE();
		PRINT 'ERROR NUMBER:'+ CAST( ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR SEVERITY'+ CAST (ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'ERROR STATE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT 'ERROR PRROCEDURE' + ERROR_PROCEDURE();
		PRINT 'ERROR LINE' + CAST(ERROR_LINE() AS NVARCHAR);
	END CATCH
END
