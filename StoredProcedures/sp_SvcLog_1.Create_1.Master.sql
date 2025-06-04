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
--
--				Also: Creates Table Index Partitions, Indexes, Trigers, and view for each table
-- =============================================================================================================

CREATE or ALTER PROCEDURE [dbo].[sp_SvcLog_1.Create_1.Master] @Run_id INT = 0 OUTPUT --Accepts input, defaults to 0 if not provided
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @CurrentDate DATE = GETDATE();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @SourceServer NVARCHAR(255);
    DECLARE @SourceDatabase NVARCHAR(255);
    DECLARE @SourceTableName NVARCHAR(255);
    DECLARE @PartitionColumn NVARCHAR(255);
    DECLARE @MinValue DATETIME;
    DECLARE @StartTime DATETIME = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss');
    DECLARE @LoadTime INT;
    DECLARE @ViewName NVARCHAR(255);
    DECLARE @ViewSQL NVARCHAR(MAX);
    DECLARE @TableNamePrefix NVARCHAR(255);
    DECLARE @SourceStartDT DATETIME;
    DECLARE @TableDistroLvl INT;
    DECLARE @TableList NVARCHAR(MAX) = '';
    DECLARE @TotalRowCount INT = 0;
    DECLARE @ReturnCode INT = 0;
    IF @Run_id = 0
    BEGIN
        SELECT @Run_id = ABS(CHECKSUM(NewId())) % 999999999;
    END
    DECLARE @TableName NVARCHAR(200);
    DECLARE @PartitionFunctionName NVARCHAR(100);
    DECLARE @PartitionSchemeName NVARCHAR(100);
    DECLARE @SourceEndDT DATE;
    DECLARE @PartitionType INT;
    DECLARE @ViewDefinition NVARCHAR(MAX);
    DECLARE @TableDefinition NVARCHAR(MAX);
    DECLARE @PrimaryKeyDefinition NVARCHAR(MAX);
    DECLARE @NonClusteredIndexDefinition NVARCHAR(MAX);
    DECLARE @InsertHistoryTriggerDefinition NVARCHAR(MAX);
    DECLARE @DeleteHistoryTriggerDefinition NVARCHAR(MAX);
    BEGIN TRY
        PRINT '----------------------------- Table Creation/Maintenance -----------------------------'
        DECLARE create_cursor CURSOR FOR
        SELECT TableNamePrefix,
               TableName,
               PartitionFunctionName,
               PartitionSchemeName,
               MAX(SourceStartDT),
               MAX(SourceEndDT),
               CASE
                   WHEN TableDistroLvl = 3
                        and PartitionFunctionName LIKE '%Monthly' THEN 3
                   WHEN TableDistroLvl = 2
                        and PartitionFunctionName LIKE '%Daily' THEN 2
                   WHEN TableDistroLvl = 2
                        and PartitionFunctionName LIKE '%Weekly' THEN 1
               END PartitionType
        FROM temp_SvcLog_1_Create ct
        GROUP BY TableNamePrefix,
                 TableName,
                 PartitionFunctionName,
                 PartitionSchemeName,
                 CASE
                     WHEN TableDistroLvl = 3
                        and PartitionFunctionName LIKE '%Monthly' THEN 3
					 WHEN TableDistroLvl = 2
                          and PartitionFunctionName LIKE '%Daily' THEN 2
                     WHEN TableDistroLvl = 2
                          and PartitionFunctionName LIKE '%Weekly' THEN 1
                 END;
        OPEN create_cursor;
        FETCH NEXT FROM create_cursor
        INTO @TableNamePrefix,
             @TableName,
             @PartitionFunctionName,
             @PartitionSchemeName,
             @SourceStartDT,
             @SourceEndDT,
             @PartitionType;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Go!
            -- Create/Alter/Extend index Partitions.
            BEGIN TRY
                EXEC [dbo].[sp_SvcLog_1.Create_2.idxPartition] @PartitionFunctionName = @PartitionFunctionName,
                                                               @PartitionSchemeName = @PartitionSchemeName,
                                                               @AddDate = @SourceStartDT,
                                                               @PartitionType = @PartitionType,
                                                               @Run_id = @Run_id,
                                                               @TableName = @TableName;
            END TRY
            BEGIN CATCH
                SELECT @ErrorMessage = ERROR_MESSAGE(),
                       @ErrorSeverity = ERROR_SEVERITY(),
                       @ErrorState = ERROR_STATE();
                PRINT 'Error occurred while extending Partition Function:';
                PRINT 'Error ' + CAST(@ErrorSeverity AS NVARCHAR(10)) + ', State ' + CAST(@ErrorState AS NVARCHAR(10))
                      + ': ' + @ErrorMessage;
            END CATCH;
            SELECT @TableDefinition = TableDefinition,
                   @PrimaryKeyDefinition = PrimaryKeyDefinition,
                   @NonClusteredIndexDefinition = NonClusteredIndexDefinition,
                   @PartitionColumn = PartitionColumn,
                   @InsertHistoryTriggerDefinition = InsertHistoryTriggerDefinition,
                   @DeleteHistoryTriggerDefinition = DeleteHistoryTriggerDefinition,
                   @TableDistroLvl = TableDistroLvl
            FROM addin_SvcLogTableStructures
            WHERE TableNamePrefix = @TableNamePrefix;
            BEGIN TRY
                EXEC [dbo].[sp_SvcLog_1.Create_3.Table] @TableName = @TableName,
                                                        @PartitionSchemeName = @PartitionSchemeName,
                                                        @PartitionColumn = @PartitionColumn,
                                                        @SourceEndDT = @SourceEndDT,
                                                        @PartitionType = @PartitionType,
                                                        @TableDefinition = @TableDefinition,
                                                        @Run_id = @Run_id;
            END TRY
            BEGIN CATCH
                SELECT @ErrorMessage = ERROR_MESSAGE(),
                       @ErrorSeverity = ERROR_SEVERITY(),
                       @ErrorState = ERROR_STATE();
                PRINT 'Error occurred while creating Table:';
                PRINT 'Error ' + CAST(@ErrorSeverity AS NVARCHAR(10)) + ', State ' + CAST(@ErrorState AS NVARCHAR(10))
                      + ': ' + @ErrorMessage;
                CONTINUE;
            END CATCH;
            BEGIN TRY
                IF @TableName != 'rpt_trigger_log_detail'
                BEGIN
                    EXEC [dbo].[sp_SvcLog_1.Create_4.tblIndex] @TableName = @TableName,
                                                               @PartitionSchemeName = @PartitionSchemeName,
                                                               @PartitionColumn = @PartitionColumn,
                                                               @PrimaryKeyDefinition = @PrimaryKeyDefinition,
                                                               @NonClusteredIndexDefinition = @NonClusteredIndexDefinition,
                                                               @Run_id = @Run_id;
                END;
            END TRY
            BEGIN CATCH
                SELECT @ErrorMessage = ERROR_MESSAGE(),
                       @ErrorSeverity = ERROR_SEVERITY(),
                       @ErrorState = ERROR_STATE();
                PRINT 'Error occurred while creating an table Index:';
                PRINT 'Error ' + CAST(@ErrorSeverity AS NVARCHAR(10)) + ', State ' + CAST(@ErrorState AS NVARCHAR(10))
                      + ': ' + @ErrorMessage;
            END CATCH;
            BEGIN TRY
                EXEC [dbo].[sp_SvcLog_1.Create_5.Trigger] @TableName = @TableName,
                                                          @PartitionSchemeName = @PartitionSchemeName,
                                                          @PartitionColumn = @PartitionColumn,
                                                          @PartitionType = @PartitionType,
                                                          @InsertHistoryTriggerDefinition = @InsertHistoryTriggerDefinition,
                                                          @DeleteHistoryTriggerDefinition = @DeleteHistoryTriggerDefinition,
                                                          @Run_id = @Run_id;
            END TRY
            BEGIN CATCH
                SELECT @ErrorMessage = ERROR_MESSAGE(),
                       @ErrorSeverity = ERROR_SEVERITY(),
                       @ErrorState = ERROR_STATE();
                PRINT 'Error occurred while creating table Trigger:';
                PRINT 'Error ' + CAST(@ErrorSeverity AS NVARCHAR(10)) + ', State ' + CAST(@ErrorState AS NVARCHAR(10))
                      + ': ' + @ErrorMessage;
                CONTINUE;
            END CATCH;
            PRINT '----------------------- ' + QUOTENAME(@TableName)
                  + ' Table and Assets created/altered -----------------------'
            FETCH NEXT FROM create_cursor
            INTO @TableNamePrefix,
                 @TableName,
                 @PartitionFunctionName,
                 @PartitionSchemeName,
                 @SourceStartDT,
                 @SourceEndDT,
                 @PartitionType;
            PRINT '----------------------------- End Table Creation/Maintenance -----------------------------'
        END
        CLOSE create_cursor;
        DEALLOCATE create_cursor;
    END TRY
    BEGIN CATCH
        CLOSE create_cursor;
        DEALLOCATE create_cursor;
        PRINT 'Nope Tables'
    END CATCH
    BEGIN TRY
        PRINT '----------------------- Begin View Creation/Maintenance -----------------------'
        DECLARE view_cursor CURSOR FOR
        SELECT TableNamePrefix,
               TableName,
               ViewDefinition,
               TableDistroLvl
        FROM
        (
            SELECT a.TableNamePrefix,
                   a.TableName,
                   b.ViewDefinition,
                   a.TableDistroLvl
            FROM temp_SvcLog_1_Create a
                JOIN addin_SvcLogTableStructures b
                    on a.TableNamePrefix = b.TableNamePrefix
            GROUP BY a.TableNamePrefix,
                     a.TableName,
                     b.ViewDefinition,
                     a.TableDistroLvl
        ) x;
        OPEN view_cursor;
        FETCH NEXT FROM view_cursor
        INTO @TableNamePrefix,
             @TableName,
             @ViewDefinition,
             @TableDistroLvl;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                IF @TableDistroLvl = 0
                BEGIN
                    EXEC [dbo].[sp_SvcLog_1.Create_6.View] @TableName = @TableName,
                                                           @TableNamePrefix = @TableNamePrefix,
                                                           @ViewDefinition = @ViewDefinition,
                                                           @TableDistroLvl = @TableDistroLvl,
                                                           @Run_id = @Run_id;
                END
                ELSE
                BEGIN
                    SELECT @TableList
                        = STRING_AGG(
                                        'SELECT *, ''' + TableName + ''' as vwName FROM ' + QUOTENAME(TableName),
                                        ' UNION ALL '
                                    ) --Roll em' up! (UNION ALL for the tech minded)
                    FROM temp_SvcLog_1_Create
                    WHERE TableNamePrefix = @TableNamePrefix;
                    EXEC [dbo].[sp_SvcLog_1.Create_6.View] @TableName = @TableName,
                                                           @TableNamePrefix = @TableNamePrefix,
                                                           @TableList = @TableList,
                                                           @ViewDefinition = @ViewDefinition,
                                                           @TableDistroLvl = @TableDistroLvl,
                                                           @Run_id = @Run_id;
                END;
            END TRY
            BEGIN CATCH
                SELECT @ErrorMessage = ERROR_MESSAGE(),
                       @ErrorSeverity = ERROR_SEVERITY(),
                       @ErrorState = ERROR_STATE();
                PRINT 'Error occurred while creating View:';
                PRINT 'Error ' + CAST(@ErrorSeverity AS NVARCHAR(10)) + ', State ' + CAST(@ErrorState AS NVARCHAR(10))
                      + ': ' + @ErrorMessage;
            END CATCH;
            FETCH NEXT FROM view_cursor
            INTO @TableNamePrefix,
                 @TableName,
                 @ViewDefinition,
                 @TableDistroLvl;
        END
        CLOSE view_cursor;
        DEALLOCATE view_cursor;
        PRINT '----------------------- END View Creation/Maintenance -----------------------'
    END TRY
    BEGIN CATCH
        CLOSE view_cursor;
        DEALLOCATE view_cursor;
    END CATCH
    BEGIN TRY
        EXEC [dbo].[sp_SvcLog_1.Create_7.DropExpired] @Run_id = @Run_id;
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();
        PRINT 'Error occurred while creating View:';
        PRINT 'Error ' + CAST(@ErrorSeverity AS NVARCHAR(10)) + ', State ' + CAST(@ErrorState AS NVARCHAR(10)) + ': '
              + @ErrorMessage;
    END CATCH;
    BEGIN TRY
        PRINT '----------------------------- ~fin~ maintenance! -----------------------------'
        SET @LoadTime = DATEDIFF(millisecond, @StartTime, GETDATE())
        DECLARE @hr varchar(2);
        DECLARE @min varchar(2);
        DECLARE @sec varchar(2);
        DECLARE @ms varchar(10);
        select @hr = @LoadTime / 3600000,
               @min = (@LoadTime - ((@LoadTime / 3600000) * 3600000)) / 60000,
               @sec = (@LoadTime - (((@LoadTime) / 60000) * 60000)) / 1000,
               @ms = @LoadTime - (((@LoadTime) / 1000) * 1000)
        PRINT CHAR(13) + 'Start Time: ' + CAST(FORMAT(@StartTime, 'yyyy-MM-dd HH:mm:ss') as NVARCHAR(20));
        PRINT '-------------------------------------------------------------------'
        PRINT 'Table Creation Runtime: ' + CAST(@LoadTime as NVARCHAR(10)) + ' milliseconds';
        PRINT 'Runtime Breakdown: ' + CHAR(13) + 'Minutes: ' + CAST(@min as NVARCHAR(2)) + CHAR(13) + 'Seconds: '
              + CAST(@sec as NVARCHAR(2)) + CHAR(13) + 'Milliseconds: ' + CAST(@ms as NVARCHAR(2));
        PRINT '-------------------------------------------------------------------'
        PRINT 'End Time: ' + CAST(FORMAT(GetDate(), 'yyyy-MM-dd HH:mm:ss') as NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();
        PRINT 'Error occurred during processing:';
        PRINT 'Error ' + CAST(@ErrorSeverity AS NVARCHAR(10)) + ', State ' + CAST(@ErrorState AS NVARCHAR(10)) + ': '
              + @ErrorMessage;
    END CATCH;
    RETURN @Run_id
END;
GO