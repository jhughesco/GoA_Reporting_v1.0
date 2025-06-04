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
CREATE or ALTER PROCEDURE [dbo].[sp_SvcLog_3.Purge_3.Delete]
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
    DECLARE @Deleted_Rows INT = 1;

    BEGIN TRY
        WHILE (@Deleted_Rows > 0)
        BEGIN
            BEGIN TRANSACTION

            SET @PurgeSQL
                = 'DELETE TOP (10000) FROM ' + QUOTENAME(@TableName) + ' WHERE ' + @PartitionColumn + ' <= '''
                  + CAST(CAST(@SourceEndDT as DATETIME2) as NVARCHAR(23)) + ''';';
            EXEC sp_executesql @PurgeSQL;

            SET @Deleted_Rows = @@ROWCOUNT;

			IF @Deleted_Rows > 0
			BEGIN
				SET @TotalRowCount = @TotalRowCount + @Deleted_Rows;
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
            SET @ErrorMessage = 'DELETE Expired rows Success!';
            SET @ErrorCode = 0;
			EXEC dbo.sp_fnLogSPRunHistory @Run_id = @Run_id,
                                          @TableAffected = @TableName,
                                          @RowsAffected = @TotalRowCount,
                                          @Action = 'PURGE: DELETE Exp.',
                                          @ErrorCode = @ErrorCode,
                                          @ErrorMessage = @ErrorMessage;
        END
        ELSE
        BEGIN
            SET @ErrorMessage = '0 exp. rows!';
            SET @ErrorCode = 0;
            EXEC dbo.sp_fnLogSPRunHistory @Run_id = @Run_id,
                                          @TableAffected = @TableName,
                                          @RowsAffected = @TotalRowCount,
                                          @Action = 'PURGE: DELETE 0 Exp.',
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
                                      @Action = 'ERROR',
                                      @ErrorCode = @ErrorNumber,
                                      @ErrorMessage = @ErrorMessage;
    END CATCH;

    SET @RowCount = @TotalRowCount;
    RETURN @RowCount;
END;
GO