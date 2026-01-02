
/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- We are using the path '/var/opt/mssql/csv/cust_info.csv' because the CSV files
-- were copied to a Docker volume (csv-data) and mounted inside the SQL Server container.
-- The SQL Server inside the container **cannot access your Mac's local folders directly**, 
-- so we need to point to the path inside the container where the volume is mounted.

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();
    BEGIN TRY
        PRINT '===============================';
        PRINT 'Loading Bronze Layer';
        PRINT '===============================';

        PRINT '-------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data Into Table crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/csv/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------'
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data Into Table crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/csv/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting Data Into Table crm_sales_details';    
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/csv/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------'

        PRINT '-------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Data Into Table erp_cust_az12';    
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/csv/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data Into Table erp_loc_a101';   
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/csv/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into Table erp_px_cat_g1v2';   
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/csv/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------'
        
        SET @batch_end_time = GETDATE();
        PRINT '================================'
        PRINT '>> Loading Bronze Layer is Completed'
        PRINT 'Total Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================'

    END TRY
    BEGIN CATCH
        PRINT '================================'
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================'
    END CATCH
END
