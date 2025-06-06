/*
  ------------------------------------------------------------------------------
  Script Name   : 01_Database_Create.sql
  Purpose       : Creates the GADATA_RPT database with specific filegroups, 
          file locations, sizes, and options.
  ------------------------------------------------------------------------------
  Details:
    - Uses the [master] database context to create a new database [GADATA_RPT].
    - PRIMARY filegroup:
      * Data file: GADATA_RPT.mdf
      * Location: E:\SQL-Data\User\MSSQL\
      * Initial size: 5GB
      * Unlimited max size, 1GB file growth increments.
    - Full Text Index filegroup:
      * Filegroup: GADATA_RPT_FullTextIndex
      * Data file: GADATA_RPT_FTI.ndf
      * Location: I:\SQL-Data\User\MSSQL\
      * Initial size: 10GB
      * Max size: 20GB
      * File growth: 524,288KB (~512MB)
    - FILESTREAM filegroup:
      * Filegroup: GADATA_RPT_FileStreamGroup
      * File: GADATA_RPT_FS
      * Location: I:\SQL-Data\User\MSSQL\
      * Used for storing FILESTREAM data.
    - Transaction log:
      * Log file: GADATA_RPT_log.ldf
      * Location: E:\SQL-Log\
      * Initial size: 5696KB
      * Max size: 20GB
      * File growth: 10%
    - Additional Options:
      * CATALOG_COLLATION set to DATABASE_DEFAULT.
      * LEDGER option is OFF.
      * FILESTREAM non-transacted access is FULL.
      * FILESTREAM directory name: SQL-FS.
  ------------------------------------------------------------------------------
  Notes:
    - Ensure the specified file paths exist and SQL Server service account 
    has necessary permissions.
    - Adjust file sizes and growth settings as per storage and performance needs.
    - FILESTREAM and Full Text Index features require appropriate SQL Server configuration.
  ------------------------------------------------------------------------------
*/
USE [master]
GO

CREATE DATABASE [GADATA_RPT] 
ON PRIMARY 
( NAME = N'GADATA_RPT', 
  FILENAME = N'E:\SQL-Data\User\MSSQL\GADATA_RPT.mdf' , 
  SIZE = 5GB , 
  MAXSIZE = UNLIMITED, 
  FILEGROWTH = 1GB ), -- Changed to fixed growth
FILEGROUP [GADATA_RPT_FullTextIndex] 
( NAME = N'GADATA_RPT_FTI', 
  FILENAME = N'I:\SQL-Data\User\MSSQL\GADATA_RPT_FTI.ndf' , 
  SIZE = 10GB , 
  MAXSIZE = 20GB , 
  FILEGROWTH = 524288KB ),
 FILEGROUP [GADATA_RPT_FileStreamGroup] CONTAINS FILESTREAM (
    NAME = N'GADATA_RPT_FS', 
	FILENAME = N'I:\SQL-Data\User\MSSQL\GADATA_RPT_FS')
 LOG ON 
( NAME = N'GADATA_RPT_log', 
  FILENAME = N'E:\SQL-Log\GADATA_RPT_log.ldf' ,
  SIZE = 5696KB , 
  MAXSIZE = 20GB ,
  FILEGROWTH = 10%)
 WITH CATALOG_COLLATION = DATABASE_DEFAULT, LEDGER = OFF, FILESTREAM (NON_TRANSACTED_ACCESS = FULL, DIRECTORY_NAME = N'SQL-FS')
GO