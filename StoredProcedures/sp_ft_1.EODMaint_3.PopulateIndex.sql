USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_1.EODMaint_3.PopulateIndex]
    @full_populate BIT = 0,
	@Run_id INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        DECLARE 
            @start DATETIME = GETDATE(),
            @population_status INT,
            @status_msg NVARCHAR(100);
		IF @Run_id = 0
		BEGIN
			SELECT @Run_id = ABS(CHECKSUM(NewId())) % 999999999;
		END
        -- Check current population status
        SELECT @population_status = status
        FROM sys.dm_fts_index_population
		WHERE table_id = OBJECT_ID('ft_JobLogsFT');

        -- Proceed only if no active population (status 0 = idle)
        IF @population_status = 0
        BEGIN
            IF @full_populate = 1
                ALTER FULLTEXT INDEX ON ft_JobLogsFT START FULL POPULATION;
            ELSE
                ALTER FULLTEXT INDEX ON ft_JobLogsFT START INCREMENTAL POPULATION;

            SET @status_msg = CASE @full_populate WHEN 1 THEN 'Full' ELSE 'Incremental' END;
        END
        ELSE
        BEGIN
            SET @status_msg = 'Skipped: Population already in progress (Status ' 
                            + CAST(@population_status AS NVARCHAR(2)) + ')';
        END

        -- Log result
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
        VALUES (@Run_id, 1, @status_msg, DATEDIFF(ms, @start, GETDATE()));
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
        VALUES (@Run_id, 4, CONCAT('Index Population Collection Error: ', ERROR_MESSAGE(), ' (Line ', ERROR_LINE(), ')'), DATEDIFF(ms, @start, GETDATE()));
        THROW;
    END CATCH;
END