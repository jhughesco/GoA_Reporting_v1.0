USE [GADATA_RPT]
GO
-- ==========================================================================================
-- Author:		JHughes
-- Create date: 12/2024
-- Description:	Inserts records from GoA App DB.  			
--				Distributes service log data into smaller tables for consumption, 
--					see addin_SvcLogTableStructures for specifications.
-- ==========================================================================================

CREATE or ALTER PROCEDURE [dbo].[sp_SvcLog_2.Load_1.Master]
    @TableNamePrefix NVARCHAR(200) = 'rpt_%',
    @Caller NVARCHAR(2) = 'sp' -- PowerShell use 'ps'.
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentDate DATE = GETDATE();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorNumber INT;
    DECLARE @ErrorState INT;
    DECLARE @ErrorLine INT;
    DECLARE @ErrorProcedure NVARCHAR(200);
    DECLARE @RowCount INT = 0;
    DECLARE @TotalRowCount INT = 0;
    DECLARE @DestMaxDT DATETIME;
    DECLARE @TableName NVARCHAR(255);
    DECLARE @SourceServer NVARCHAR(128);
    DECLARE @SourceDatabase NVARCHAR(128);
    DECLARE @SourceTableName NVARCHAR(128);
    DECLARE @SourceStartDT DATETIME2;
    DECLARE @SourceEndDT DATETIME2;
    DECLARE @PartitionColumn NVARCHAR(128);
    DECLARE @IDColumnName NVARCHAR(128);
    DECLARE @TableDistroLvl INT;
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @MinValue DATETIME;
    DECLARE @Run_id INT;
    SELECT @Run_id = ABS(CHECKSUM(NewId())) % 999999999;

    DECLARE @StartTime NVARCHAR(20) = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss');
    DECLARE @EndTime NVARCHAR(20);
    DECLARE @LoadStartTime NVARCHAR(20);
    DECLARE @LoadEndTime NVARCHAR(20);
    DECLARE @LoadTime INT;
    DECLARE @InsertLoadTime INT;

    BEGIN TRY
        SELECT TableNamePrefix,
               TableName,
               SourceServer,
               SourceDatabase,
               SourceTableName,
               SourceStartDT,
               SourceEndDT,
               PartitionColumn,
               IDColumnName,
               TableDistroLvl,
               @Run_id as Run_id,
               @Caller as Caller
        INTO ##TempInsertTables
        FROM temp_SvcLog_2_Load;

        IF @Caller = 'sp'
        BEGIN
            DECLARE load_cursor CURSOR FOR
            SELECT TableNamePrefix,
                   TableName,
                   SourceServer,
                   SourceDatabase,
                   SourceTableName,
                   SourceStartDT,
                   SourceEndDT,
                   PartitionColumn,
                   IDColumnName,
                   TableDistroLvl
            FROM temp_SvcLog_2_Load;

            OPEN load_cursor;

            FETCH NEXT FROM load_cursor
            INTO @TableNamePrefix,
                 @TableName,
                 @SourceServer,
                 @SourceDatabase,
                 @SourceTableName,
                 @SourceStartDT,
                 @SourceEndDT,
                 @PartitionColumn,
                 @IDColumnName,
                 @TableDistroLvl;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    EXEC [dbo].[sp_SvcLog_2.Load_2.Insert] @TableName = @TableName,
                                                           @Caller = @Caller,
                                                           @Run_id = @Run_id,
                                                           @RowCount = @RowCount OUTPUT;

                    SET @TotalRowCount = @TotalRowCount + @RowCount;
                    PRINT 'Table LOAD - INSERT: [' + CAST(@RowCount as NVARCHAR(10)) + '] rows inserted into '
                          + QUOTENAME(@TableName) + '';
                END TRY
                BEGIN CATCH
                    SELECT @ErrorSeverity = ERROR_SEVERITY(),
                           @ErrorNumber = ERROR_NUMBER(),
                           @ErrorMessage = ERROR_MESSAGE(),
                           @ErrorState = ERROR_STATE();
                    PRINT 'Table LOAD - Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + ', State '
                          + CAST(@ErrorState AS NVARCHAR(10)) + ': ' + @ErrorMessage;
                END CATCH;

                FETCH NEXT FROM load_cursor
                INTO @TableNamePrefix,
                     @TableName,
                     @SourceServer,
                     @SourceDatabase,
                     @SourceTableName,
                     @SourceStartDT,
                     @SourceEndDT,
                     @PartitionColumn,
                     @IDColumnName,
                     @TableDistroLvl;
            END;
        END;

        SET @LoadTime = DATEDIFF(millisecond, @StartTime, GETDATE())

        DECLARE @hr varchar(2);
        DECLARE @min varchar(2);
        DECLARE @sec varchar(2);
        DECLARE @ms varchar(10);

        select @hr = @LoadTime / 3600000,
               @min = (@LoadTime - ((@LoadTime / 3600000) * 3600000)) / 60000,
               @sec = (@LoadTime - (((@LoadTime) / 60000) * 60000)) / 1000,
               @ms = @LoadTime - (((@LoadTime) / 1000) * 1000),
               @EndTime = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss');

        PRINT CHAR(13) + 'Start Time: ' + CAST(@StartTime as NVARCHAR(20));
        PRINT '-----------------------------------------------------------';
        PRINT 'Source Table/s Replication Runtime: ' + CAST(@LoadTime as NVARCHAR(10)) + ' milliseconds';
        PRINT 'Runtime Breakdown: ' + CHAR(13) + 'Minutes: ' + CAST(@min as NVARCHAR(2)) + CHAR(13) + 'Seconds: '
              + CAST(@sec as NVARCHAR(2)) + CHAR(13) + 'Milliseconds: ' + CAST(@ms as NVARCHAR(2));
        PRINT '-----------------------------------------------------------';
        PRINT 'Total Rows Inserted: ' + CAST(@TotalRowCount as NVARCHAR(14));
        PRINT '-----------------------------------------------------------';
        PRINT 'End Time: ' + CAST(@EndTime as NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE(),
               @ErrorLine = ERROR_LINE(),
               @ErrorProcedure = ERROR_PROCEDURE();

        INSERT INTO dbo.addin_LogSProcInsertHistory
        (
            Run_id,
            TableAffected,
            RowsAffected,
            Action,
            ActionDT
        )
        VALUES
        (@Run_id, @TableNamePrefix, NULL, CONCAT('Error: ', @ErrorMessage), GETDATE());
    END CATCH;
END;
GO
