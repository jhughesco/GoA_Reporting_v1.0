USE [GADATA_RPT]
GO
/****** Object:  StoredProcedure [dbo].[sp_ft_1.EODMaint_4.idxPartition]    Script Date: 6/2/2025 12:11:06 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_ft_1.EODMaint_4.idxPartition]
    @Run_id INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start DATETIME = GETDATE();
    DECLARE @today DATE = CAST(GETDATE() AS DATE);
    DECLARE @new_boundary DATE = DATEADD(DAY, 1, @today);
    DECLARE @oldest_boundary DATE;
    DECLARE @status_msg NVARCHAR(500);
    DECLARE @action_taken BIT = 0;
    DECLARE @partition_scheme SYSNAME = 'ps_metadata_daily';
    DECLARE @filegroup SYSNAME = '[PRIMARY]'; -- Replace with your actual filegroup
	DECLARE @split_sql NVARCHAR(MAX);
	DECLARE @merge_sql NVARCHAR(MAX);

    IF @Run_id = 0
        SELECT @Run_id = ABS(CHECKSUM(NewId())) % 999999999;

    BEGIN TRY
        -- 1. Find oldest boundary
        SELECT TOP 1 @oldest_boundary = CONVERT(DATE, value)
        FROM sys.partition_range_values
        WHERE function_id = (SELECT function_id FROM sys.partition_functions WHERE name = 'pf_metadata_daily')
        ORDER BY value ASC;

        -- 2. SPLIT: Add tomorrow's partition
        IF NOT EXISTS (
            SELECT 1
            FROM sys.partition_range_values
            WHERE function_id = (SELECT function_id FROM sys.partition_functions WHERE name = 'pf_metadata_daily')
              AND CONVERT(DATE, value) = @new_boundary
        )
        BEGIN
            -- Always set NEXT USED before SPLIT
			EXEC('ALTER PARTITION SCHEME ' + @partition_scheme + ' NEXT USED ' + @filegroup + ';');
                
            INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
            VALUES (@Run_id, 12, 'Set NEXT USED filegroup for ' + @partition_scheme, DATEDIFF(ms, @start, GETDATE()));
            
			-- Convert the date to strings before concatenation
			DECLARE @new_boundary_str VARCHAR(23) = CONVERT(VARCHAR(23), @new_boundary, 120);
			DECLARE @oldest_boundary_str VARCHAR(23) = CONVERT(VARCHAR(23), @oldest_boundary, 120);

            -- Split partition
            SET @split_sql = 'ALTER PARTITION FUNCTION pf_metadata_daily() SPLIT RANGE (''' + @new_boundary_str + ''');';
			EXEC(@split_sql);

            SET @status_msg = 'Split partition for: ' + CONVERT(CHAR(10), @new_boundary, 120);
            INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
            VALUES (@Run_id, 9, @status_msg, DATEDIFF(ms, @start, GETDATE()));
            
            SET @action_taken = 1;
        END

        -- 3. MERGE: Remove partitions older than 16 months
        IF @oldest_boundary < DATEADD(MONTH, -16, @today)
        BEGIN
            SET @merge_sql = 'ALTER PARTITION FUNCTION pf_metadata_daily() MERGE RANGE (''' + @oldest_boundary_str + ''');';
            EXEC(@merge_sql);

            SET @status_msg = 'Merged oldest partition: ' + CONVERT(CHAR(10), @oldest_boundary, 120);
            INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
            VALUES (@Run_id,10, @status_msg, DATEDIFF(ms, @start, GETDATE()));
            SET @action_taken = 2;
        END

        -- Log if no action
        IF @action_taken = 0
        BEGIN
            INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
            VALUES (@Run_id, 11, 'No partition changes needed', DATEDIFF(ms, @start, GETDATE()));
        END
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
        VALUES (@Run_id, 4, 
                'Partition Maint. Error: ' + ERROR_MESSAGE() + ' (Line ' + CAST(ERROR_LINE() AS NVARCHAR) + ')', 
                DATEDIFF(ms, @start, GETDATE()));
        THROW;
    END CATCH;
END
GO