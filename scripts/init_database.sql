/* 
Create Database and Schemas

Script purpose :
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the databse exists, it is dropped and recreated. Additionally, thye sript sets up three schemas 
  within bthe database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution and 
  ensured you have proper backup before running this script.
*/

Use master 
go

-- Drop and recreate the 'DataWarehouse' database
if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter database DataWarehouse 
	set single_user with rollback immediate
	drop database DataWarehouse
end
go

-- Create Database 'DataWarehouse'

USE master;

create database DataWarehouse  -- ; optional at end of the sentence

USE DataWarehouse;
go
create schema bronze;
go
create schema silver;
go
create schema gold;
go
