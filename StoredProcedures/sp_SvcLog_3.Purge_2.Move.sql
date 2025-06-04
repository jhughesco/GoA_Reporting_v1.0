USE [GADATA_RPT]
GO
-- ==========================================================================================
-- Author:		JHughes
-- Create date: 12/2024
-- Description:	Inserts records from GoA App DB.  			
--				Distributes service log data into smaller tables for consumption, 
--					see addin_SvcLogTableStructures for specifications.
--             *Calls: LOAD class procedures
-- ==========================================================================================
CREATE or ALTER PROCEDURE [dbo].[sp_SvcLog_3.Purge_2.Move]
    @TableName NVARCHAR(255),
    @MoveToTableName NVARCHAR(255),
    @PartitionColumn NVARCHAR(128),
    @SourceStartDT DATETIME2,
    @SourceEndDT DATETIME2,
    @IDColumnName NVARCHAR(128),
    @TableDistroLvl INT,
    @WeekTbl INT,
    @Run_id INT = 0,
    @RowCount INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorNumber INT;
    DECLARE @ErrorCode INT;
    DECLARE @TotalRowCount INT = 0;
    DECLARE @PurgeSQL NVARCHAR(MAX);
    DECLARE @Inserted_Rows INT = 1;

    BEGIN TRY
        WHILE (@Inserted_Rows > 0)
        BEGIN
            BEGIN TRANSACTION

            SET @PurgeSQL
                = 'INSERT INTO ' + QUOTENAME(@MoveToTableName) + ' SELECT TOP (10000) * FROM ' + QUOTENAME(@TableName)
                  + ' s' + ' WHERE s.' + @PartitionColumn + ' <= '''
                  + CAST(CAST(@SourceEndDT as DATETIME2) as NVARCHAR(23)) + ''' AND MONTH(' + @PartitionColumn
                  + ') = MONTH(''' + CAST(CAST(@SourceEndDT as DATETIME2) as NVARCHAR(23)) + ''')'
                  + ' AND NOT EXISTS (' + ' SELECT 1 FROM ' + QUOTENAME(@MoveToTableName) + ' d ' + ' WHERE s.'
                  + QUOTENAME(@IDColumnName) + ' = d.' + QUOTENAME(@IDColumnName) + ');';

			PRINT @PurgeSQL
            EXEC sp_executesql @PurgeSQL;

            SET @Inserted_Rows = @@ROWCOUNT;
            
			IF @Inserted_Rows > 0
			BEGIN
				SET @TotalRowCount = @TotalRowCount + @Inserted_Rows;
			END
			ELSE
			BEGIN
				SET @TotalRowCount = @TotalRowCount + 0;
			END

            IF XACT_STATE() = 1
            BEGIN
                COMMIT TRANSACTION
            END
            ELSE
            BEGIN
                ROLLBACK TRANSACTION
                RETURN
            END

            CHECKPOINT
        END;


        IF @TotalRowCount > 0
        BEGIN
            SET @ErrorMessage = 'MOVE (INSERT) Expired Success!';
            SET @ErrorCode = 0;
			EXEC dbo.sp_fnLogSPRunHistory @Run_id = @Run_id,
                                          @TableAffected = @MoveToTableName,
                                          @RowsAffected = @TotalRowCount,
                                          @Action = 'PURGE: INSERT Exp.',
                                          @ErrorCode = @ErrorCode,
                                          @ErrorMessage = @ErrorMessage;
        END
        ELSE
        BEGIN
            SET @ErrorMessage = 'Move 0 exp. rows!';
            SET @ErrorCode = 0;
			EXEC dbo.sp_fnLogSPRunHistory @Run_id = @Run_id,
                                          @TableAffected = @MoveToTableName,
                                          @RowsAffected = @TotalRowCount,
                                          @Action = 'PURGE: INSERT 0 exp.',
                                          @ErrorCode = @ErrorCode,
                                          @ErrorMessage = @ErrorMessage;
        END;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION
        END
        SET @ErrorMessage
            = 'Destination Table does not EXIST!  Run: sp_SvcLog_Create_1.Master ASAP.  ' + ERROR_MESSAGE();
        SET @ErrorNumber = 50000 + @ErrorCode;
        EXEC dbo.sp_fnLogSPRunHistory @Run_id = @Run_id,
                                      @TableAffected = @TableName,
                                      @RowsAffected = NULL,
                                      @Action = 'PURGE: ERROR',
                                      @ErrorCode = @ErrorNumber,
                                      @ErrorMessage = @ErrorMessage;
    END CATCH;

    SET @RowCount = @TotalRowCount;
    RETURN @RowCount;
END;
GO