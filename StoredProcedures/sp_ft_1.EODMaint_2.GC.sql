USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_1.EODMaint_2.GC]
    @dbname NVARCHAR(128) = 'GADATA_RPT',
    @container NVARCHAR(128) = NULL,
	@Run_id INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime datetime = getdate();
	IF @Run_id = 0
    BEGIN
        SELECT @Run_id = ABS(CHECKSUM(NewId())) % 999999999;
    END
	BEGIN TRY
		DECLARE @result TABLE (
			file_name NVARCHAR(255),
			num_collected_items INT,
			num_marked_for_collection_items INT,
			num_unprocessed_items INT,
			last_collected_xact_seqno VARBINARY(16)
		);

		IF @dbname IS NULL SET @dbname = DB_NAME();
    
		INSERT INTO @result
		EXEC sp_filestream_force_garbage_collection 
			@dbname = @dbname,
			@filename = @container;

		-- Log success
		INSERT INTO dbo.ft_Maint_Log (Run_id, operation_type_id, details, duration_ms)
		SELECT @Run_id as Run_id, 2 as 'operation_type_id', CONCAT('Collected: ', num_collected_items, ' | Marked: ', num_marked_for_collection_items, ' | Unprocessed: ', num_unprocessed_items) as 'details', datediff(ms, @StartTime, getdate()) as duration_ms
		FROM @result;
	END TRY
    BEGIN CATCH
        -- Log error
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
        VALUES (@Run_id, 4, CONCAT('Garbage Collection Error: ', ERROR_MESSAGE(), ' (Line ', ERROR_LINE(), ')'), DATEDIFF(ms, @StartTime, GETDATE()));
        THROW;
    END CATCH;

END
GO
