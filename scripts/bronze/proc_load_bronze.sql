/*
=====================================================================================================================
STORED PROCEDURE: LOAD BRONZE LAYER (SOURCE -> BRONZE)
=====================================================================================================================
SCRIPT PURPOSE:
  This stored procedure loads data into the 'bronze' schema from external csv files.
  It performs the following actions:
    - Truncate bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv files into bronze tables.
PARAMETERS:
  NONE
    This stored procedure does not accpet any parameters or return any values.
USAGE EXAMPLE:
  EXEC bronze.load_bronze;
======================================================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @strt_time DATETIME, @end_time DATETIME, @bronze_layer_start_time DATETIME, @bronze_layer_end_time DATETIME; 
	BEGIN TRY
		SET @bronze_layer_start_time = GETDATE();
		PRINT '========================================================';
		PRINT 'LOADING THE BRONZE LAYER';
		PRINT '========================================================';

		PRINT '--------------------------------------------------------';
		PRINT 'LOADING THE CRM SOURCE FILES';
		PRINT '--------------------------------------------------------';
		PRINT 'TRUNCATING: bronze.crom_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'INSERTING INTO: bronze.crm_cust_info';

		SET @strt_time = GETDATE();
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Ankit Sharma\OneDrive\Desktop\data warehouse\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second, @end_time, @strt_time) AS NVARCHAR) +' seconds';


		print ' ';
		PRINT 'TRUNCATING: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'INSERTING INTO: bronze.crm_prd_info';
		SET @strt_time = GETDATE();
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Ankit Sharma\OneDrive\Desktop\data warehouse\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second, @end_time, @strt_time) AS NVARCHAR) +' seconds';


		PRINT ' ';
		PRINT 'TRUNCATING: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'INSERTING INTO: bronze.crm_sales_details';
		SET @strt_time = GETDATE();
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Ankit Sharma\OneDrive\Desktop\data warehouse\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second, @end_time, @strt_time) AS NVARCHAR) +' seconds';


		PRINT '--------------------------------------------------------';
		PRINT 'LOADING THE ERP SOURCE FILES';
		PRINT '--------------------------------------------------------';



		PRINT 'TRUNCATING: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT 'INSERTING INTO: bronze.erp_CUST_AZ12';
		SET @strt_time = GETDATE();
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\Ankit Sharma\OneDrive\Desktop\data warehouse\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second, @end_time, @strt_time) AS NVARCHAR) +' seconds';




		PRINT ' ';
		PRINT 'TRUNCATING: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT 'INSERTING INTO: bronze.erp_LOC_A101';
		SET @strt_time = GETDATE();
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\Ankit Sharma\OneDrive\Desktop\data warehouse\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second, @end_time, @strt_time) AS NVARCHAR) +' seconds';

		PRINT ' ';
		PRINT 'TRUNCATING: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT 'INSERTING INTO: bronze.erp_PX_CAT_G1V2';
		SET @strt_time = GETDATE();
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\Ankit Sharma\OneDrive\Desktop\data warehouse\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second, @end_time, @strt_time) AS NVARCHAR) +' seconds';

		SET @bronze_layer_end_time = GETDATE();
		PRINT '-----------------';
		PRINT 'Bronze layer loaded in:' + CAST(DATEDIFF(second, @bronze_layer_end_time, @bronze_layer_start_time) AS NVARCHAR)+ ' seconds';
	END TRY
	BEGIN CATCH

	PRINT '=======================================================';
	PRINT 'ERROR OCCURRED IN BRONZE LAYER';
	PRINT 'ERROR MESSAGE:'+ ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE:'+ CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE:'+ CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '=======================================================';

	END CATCH
END
