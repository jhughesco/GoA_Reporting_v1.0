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
CREATE or ALTER PROCEDURE [dbo].[sp_SvcLog_1.Create_7.DropExpired] @Run_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TABLE_NAME NVARCHAR(100);
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @ErrorNumber INT;
    DECLARE @ErrorCode INT;
    DECLARE @RowCNT INT;
    DECLARE @TotalRowCount INT;
    DECLARE @SQL NVARCHAR(MAX);

    DECLARE @StartTime DATETIME2;
    DECLARE @EndTime NVARCHAR(20);
    DECLARE @LoadStartTime NVARCHAR(20);
    DECLARE @LoadEndTime NVARCHAR(20);
    DECLARE @LoadTime INT;
    DECLARE @InsertLoadTime INT;

    BEGIN TRY
        PRINT '----------------------- Begin Drop Expired Tables -----------------------';
        DECLARE drop_cursor CURSOR FOR
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME LIKE '%rpt_%'
              AND TABLE_NAME NOT LIKE '%rpt_completed%'
              AND TABLE_NAME NOT LIKE '%vw%'
              AND TABLE_NAME NOT IN (
                                        SELECT TableName FROM temp_SvcLog_1_Create
                                    )
        ORDER BY TABLE_NAME;

        OPEN drop_cursor;

        FETCH NEXT FROM drop_cursor
        INTO @TABLE_NAME;

        IF @@FETCH_STATUS < 0
        BEGIN
            PRINT 'Action Msg: DROP TABLE - 0 expired tables.';
        END;

        WHILE @@FETCH_STATUS = 0
        BEGIN


            BEGIN TRY
                SET @SQL = 'DROP TABLE ' + QUOTENAME(@TABLE_NAME) + ';';
                EXEC sp_executesql @SQL, N'@RowCNT INT OUTPUT', @RowCNT = @RowCNT OUTPUT;
                PRINT 'Action Msg: DROP TABLE ' + @TABLE_NAME + ' (expired)';

                SET @TotalRowCount = @TotalRowCount + @RowCNT;

                EXEC dbo.sp_fnLogSPRunHistory @Run_id = @Run_id,
                                              @TableAffected = @TABLE_NAME,
                                              @RowsAffected = 0,
                                              @Action = 'CREATE: DROP TABLE';
            END TRY
            BEGIN CATCH
                SELECT @ErrorMessage = ERROR_MESSAGE(),
                       @ErrorSeverity = ERROR_SEVERITY(),
                       @ErrorState = ERROR_STATE();
                PRINT 'Error occurred while extending Partition Function:';
                PRINT 'Error ' + CAST(@ErrorSeverity AS NVARCHAR(10)) + ', State ' + CAST(@ErrorState AS NVARCHAR(10))
                      + ': ' + @ErrorMessage;
            END CATCH;

            FETCH NEXT FROM drop_cursor
            INTO @TABLE_NAME;
        END;

        PRINT '----------------------- End Drop Expired Tables -----------------------';

        CLOSE drop_cursor;
        DEALLOCATE drop_cursor;
    END TRY
    BEGIN CATCH
        CLOSE drop_cursor;
        DEALLOCATE drop_cursor;
    END CATCH

END;
GO
