/*
================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
================================================================================

Script Purpose:
    This stored procedure loads data into the 'silver' schema from bronze tables.
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Applies data cleaning and transformation logic.
    - Handles duplicates, null values, and data normalization.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;

================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver 
AS 
BEGIN 
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @start_time_batch DATETIME,
            @end_time_batch DATETIME;
    
    BEGIN TRY
        SET @start_time_batch = GETDATE();
        
        PRINT '============================================'
        PRINT 'LOADING SILVER LAYER'
        PRINT '============================================'
        PRINT '                                            '
        PRINT '============================================'
        PRINT 'LOADING CRM TABLES'
        PRINT '============================================'
        PRINT '                                            '
        
        -- Load silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: [silver].[crm_cust_info]'
        TRUNCATE TABLE [silver].[crm_cust_info]
        PRINT '>> INSERTING DATA INTO: [silver].[crm_cust_info]'
        
        INSERT INTO [silver].[crm_cust_info] ([cst_id], [cst_key], [cst_firstname], [cst_lastname], [cst_marital_status], [cst_gndr], [cst_create_date])
        SELECT
            [cst_id],
            [cst_key],
            TRIM([cst_firstname]) [cst_firstname],
            TRIM([cst_lastname]) [cst_lastname],
            CASE WHEN UPPER(TRIM([cst_marital_status])) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM([cst_marital_status])) = 'M' THEN 'Married'
                 ELSE 'n/a'
            END [cst_marital_status],
            CASE WHEN UPPER(TRIM([cst_gndr])) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM([cst_gndr])) = 'M' THEN 'Male'
                 ELSE 'n/a'
            END [cst_gndr],
            [cst_create_date]
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY [cst_id] ORDER BY [cst_create_date] DESC) AS [flag_last]
                FROM [bronze].[crm_cust_info]
            ) t 
        WHERE [flag_last] = 1 AND [cst_id] IS NOT NULL;
        
        SET @end_time = GETDATE();
        PRINT '>> LOADING TABLE DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '                                            '
        
        -- Load silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: [silver].[crm_prd_info]'
        TRUNCATE TABLE [silver].[crm_prd_info]
        PRINT '>> INSERTING DATA INTO: [silver].[crm_prd_info]'
        
        INSERT INTO [silver].[crm_prd_info] ([prd_id], [Cat_id], [prd_key], [prd_nm], [prd_cost], [prd_line], [prd_start_dt], [prd_end_dt])
        SELECT
            [prd_id],
            REPLACE(SUBSTRING([prd_key], 1, 5), '-', '_') [Cat_id],
            SUBSTRING([prd_key], 7, LEN([prd_key])) [prd_key],
            [prd_nm],
            ISNULL([prd_cost], 0) AS [prd_cost],
            CASE UPPER(TRIM([prd_line])) 
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'R' THEN 'Road'
                 WHEN 'S' THEN 'Other Sales'
                 WHEN 'T' THEN 'Touring'
                 ELSE 'n/a'
            END AS [prd_line],
            CAST([prd_start_dt] AS DATE) [prd_start_dt],
            DATEADD(DAY, -1, CAST(LEAD(CAST([prd_start_dt] AS DATE)) OVER(PARTITION BY [prd_key] ORDER BY CAST([prd_start_dt] AS DATE)) AS DATE)) AS [prd_end_dt]
        FROM [bronze].[crm_prd_info]
        ORDER BY [prd_key];
        
        SET @end_time = GETDATE();
        PRINT '>> LOADING TABLE DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '                                            '
        
        -- Load silver.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: [silver].[crm_sales_details]'
        TRUNCATE TABLE [silver].[crm_sales_details]
        PRINT '>> INSERTING DATA INTO: [silver].[crm_sales_details]'
        
        INSERT INTO [silver].[crm_sales_details] ([sls_ord_num], [sls_prd_key], [sls_cust_id], [sls_order_dt], [sls_ship_dt], [sls_due_dt], [sls_price], [sls_quantity], [sls_sales])
        SELECT
            [sls_ord_num],
            [sls_prd_key],
            [sls_cust_id],
            CASE WHEN [sls_order_dt] <= 0 OR LEN([sls_order_dt]) != 8 THEN NULL 
                 ELSE CAST(CAST([sls_order_dt] AS VARCHAR) AS DATE)
            END AS [sls_order_dt],
            CASE WHEN [sls_ship_dt] <= 0 OR LEN([sls_ship_dt]) != 8 THEN NULL
                 ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE)
            END AS [sls_ship_dt],
            CASE WHEN [sls_due_dt] <= 0 OR LEN([sls_due_dt]) != 8 THEN NULL 
                 ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE)
            END AS [sls_due_dt],
            CASE WHEN [sls_price] IS NULL OR [sls_price] <= 0 
                 THEN [sls_sales] / NULLIF([sls_quantity], 0)
                 ELSE [sls_price]
            END AS [sls_price],
            [sls_quantity],
            CASE WHEN [sls_sales] IS NULL OR [sls_sales] <= 0 OR [sls_sales] != [sls_quantity] * ABS([sls_price])
                 THEN [sls_quantity] * ABS([sls_price])
                 ELSE [sls_sales]
            END AS [sls_sales]
        FROM [bronze].[crm_sales_details];
        
        SET @end_time = GETDATE();
        PRINT '>> LOADING TABLE DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '                                            '
        
        PRINT '============================================'
        PRINT 'LOADING ERP TABLES'
        PRINT '============================================'
        PRINT '                                            '
        
        -- Load silver.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: [silver].[erp_cust_az12]'
        TRUNCATE TABLE [silver].[erp_cust_az12]
        PRINT '>> INSERTING DATA INTO: [silver].[erp_cust_az12]'
        
        INSERT INTO [silver].[erp_cust_az12] ([cid], [bdate], [gen])
        SELECT
            CASE WHEN [cid] LIKE 'NAS%' THEN SUBSTRING([cid], 4, LEN([cid]))
                 ELSE [cid]
            END AS [cid],
            CASE WHEN [bdate] > GETDATE() THEN NULL
                 ELSE [bdate]
            END AS [bdate],
            CASE WHEN UPPER(TRIM([gen])) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM([gen])) = 'M' THEN 'Male'
                 WHEN UPPER(TRIM([gen])) IS NULL THEN 'n/a'
                 ELSE 'n/a'
            END AS [gen]
        FROM [bronze].[erp_cust_az12];
        
        SET @end_time = GETDATE();
        PRINT '>> LOADING TABLE DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '                                            '
        
        -- Load silver.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: [silver].[erp_loc_a101]'
        TRUNCATE TABLE [silver].[erp_loc_a101]
        PRINT '>> INSERTING DATA INTO: [silver].[erp_loc_a101]'
        
        INSERT INTO [silver].[erp_loc_a101] ([cid], [cntry])
        SELECT
            REPLACE([cid], '-', '') [cid],
            CASE WHEN UPPER(TRIM([cntry])) = 'US' OR UPPER(TRIM([cntry])) = 'USA' THEN 'United States'
                 WHEN LEN(UPPER(TRIM([cntry]))) < 2 OR UPPER(TRIM([cntry])) IS NULL THEN 'n/a'
                 WHEN UPPER(TRIM([cntry])) = 'DE' THEN 'Germany'
                 ELSE TRIM([cntry])
            END AS [cntry]
        FROM [bronze].[erp_loc_a101];
        
        SET @end_time = GETDATE();
        PRINT '>> LOADING TABLE DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '                                            '
        
        -- Load silver.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: [silver].[erp_px_cat_g1v2]'
        TRUNCATE TABLE [silver].[erp_px_cat_g1v2]
        PRINT '>> INSERTING DATA INTO: [silver].[erp_px_cat_g1v2]'
        
        INSERT INTO [silver].[erp_px_cat_g1v2] ([id], [cat], [subcat], [maintenance])
        SELECT
            [id],
            [cat],
            [subcat],
            [maintenance]
        FROM [bronze].[erp_px_cat_g1v2];
        
        SET @end_time = GETDATE();
        PRINT '>> LOADING TABLE DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        
        SET @end_time_batch = GETDATE();
        PRINT '>>> LOADING WHOLE BATCH DURATION: ' + CAST(DATEDIFF(SECOND, @start_time_batch, @end_time_batch) AS NVARCHAR) + ' SECONDS';
        
    END TRY
    BEGIN CATCH
        PRINT '============================================';
        PRINT 'ERRORS OCCURRED DURING LOADING SILVER LAYER';
        PRINT '============================================';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR SEVERITY: ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'ERROR PROCEDURE: ' + ERROR_PROCEDURE();
        PRINT 'ERROR LINE: ' + CAST(ERROR_LINE() AS NVARCHAR);
    END CATCH
END

exec silver.load_silver
