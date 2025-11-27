/*
================================================================================
Create Database and Schemas
================================================================================

Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/

-- Switch to the master database to perform database-level operations
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
-- First, check if the database exists in the system catalog
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Set the database to single-user mode to close all existing connections
    -- This ensures no one else is using the database when we try to drop it
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    
    -- Drop the existing database
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
-- This gives us a fresh, empty database to work with
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the newly created database
-- All subsequent commands will run against this database
USE DataWarehouse;
GO

-- Create Schemas
-- Schemas help organize database objects into logical groups
-- In a data warehouse, bronze/silver/gold typically represent data refinement stages

-- Bronze schema: typically holds raw, unprocessed data as it arrives from sources
CREATE SCHEMA bronze;
GO

-- Silver schema: typically holds cleaned and validated data with some transformations applied
CREATE SCHEMA silver;
GO

-- Gold schema: typically holds business-level aggregated data ready for reporting and analytics
CREATE SCHEMA gold;
GO
