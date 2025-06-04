USE [GADATA_RPT]
GO
-- =============================================================================================================
-- Author:		JHughes
-- Create date: 12/2024
-- Description:	Creates a series of tables, see addin_SvcLogTableStructures for specifications.
--              Specs, service table size breakout (approx), to maintain logs x 16mo:
--                < 2.5 mil/rec/mo. = Single Table.  rpt(replaces dpa)_[OrigTableName] 
--                > 2.5 mil/rec/mo. < 6.5 = 16 tables. rpt(replaces dpa)_[OrigTableName]_[yyyy]_[mm]
--                > 6.5 mil/rec/mo. = 25 tables. rpt(replaces dpa)_[OrigTableName]_[yyyy]_[mm]_[WeekOfYear]
--                ** multi table sets may have 1-2 tables more than above, depending on timing.
-- =============================================================================================================
CREATE PROCEDURE [dbo].[sp_SvcLog_1.Create_3.Table]
    @TableName NVARCHAR(200),
    @PartitionSchemeName NVARCHAR(100),
    @PartitionColumn NVARCHAR(255),
    @SourceEndDT DATE,
    @PartitionType INT,
    @TableDefinition NVARCHAR(MAX),
    @Run_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @ReturnCode INT = 0;
    DECLARE @RowCount INT;
    DECLARE @SQL NVARCHAR(MAX);

    IF EXISTS (SELECT name FROM sys.tables WHERE name = @TableName)
    BEGIN
        PRINT 'Table Maint - Table ' + QUOTENAME(@TableName) + ' already exists.';
    END
    ELSE
    BEGIN
        PRINT QUOTENAME(@TableName) + ' DOES NOT EXIST, let''s create it!';
        SET @SQL
            = REPLACE(
                         REPLACE(REPLACE(@TableDefinition, '{0}', QUOTENAME(@TableName)), '{1}', @PartitionSchemeName),
                         '{2}',
                         @PartitionColumn
                     );

        BEGIN TRY
            EXEC @ReturnCode = sp_executesql @SQL;

            EXEC dbo.sp_fnLogSPRunHistory @Run_id = @Run_id,
                                          @TableAffected = @TableName,
                                          @RowsAffected = @ReturnCode,
                                          @Action = 'CREATE: CREATE TABLE',
                                          @ErrorCode = NULL,
                                          @ErrorMessage = NULL;

            PRINT 'Table Create - ' + QUOTENAME(@TableName) + ' created successfully.';
        END TRY
        BEGIN CATCH
            SELECT @ErrorMessage = ERROR_MESSAGE(),
                   @ErrorSeverity = ERROR_SEVERITY(),
                   @ErrorState = ERROR_STATE();

            EXEC dbo.sp_fnLogSPRunHistory @Run_id = @Run_id,
                                          @TableAffected = @TableName,
                                          @RowsAffected = NULL,
                                          @Action = 'CREATE: CREATE TABLE',
                                          @ErrorCode = @ErrorSeverity,
                                          @ErrorMessage = @ErrorMessage;

            PRINT 'Table Create Error - Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
            PRINT 'Table Create Error - Severity: State ' + CAST(ERROR_STATE() AS NVARCHAR(10)) + ': '
                  + ERROR_MESSAGE();
        END CATCH;
    END;
END;
GO