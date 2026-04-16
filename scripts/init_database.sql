/*
  ===========================================================================================
  CREATE DATABASE AND SCHEMAS
  ===========================================================================================
  SCRIPT PURPOSE:
    This script creates a new database named "DataWarehouse" after checking if it already exists.
    If the database exists it is dropped and recreated. Additionally the script set up three schemas
    within the database: 'bronze', 'silver' and 'gold'.

  WARNING:
    Running the script will drop the entire 'DataWarehouse' database if it exists.
    All the data in the database will be permnently deleted. Proceed with caution and ensure you have
    proper backups before running the script.
*/

USE master;
GO
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
ALTER DATABSE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
