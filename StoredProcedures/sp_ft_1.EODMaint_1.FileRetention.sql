USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_1.EODMaint_1.FileRetention]
	@RetMos INT = 16,
	@Run_id INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start DATETIME = GETDATE();
    DECLARE @cutoff DATETIME2 = DATEADD(MONTH, -@RetMos, GETDATE());
    DECLARE @deleted_count INT = 0;
	 DECLARE @deletedMetadata_count INT = 0;
    DECLARE @status_msg NVARCHAR(500);
	IF @Run_id = 0
    BEGIN
        SELECT @Run_id = ABS(CHECKSUM(NewId())) % 999999999;
    END
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Delete FileTable records older than 16 months
        DELETE FROM dbo.ft_JobLogsFT
        WHERE stream_id IN (
            SELECT stream_id
            FROM dbo.ft_Metadata
            WHERE startDT < @cutoff
        );

        SET @deleted_count = @@ROWCOUNT;

		DELETE FROM dbo.ft_Metadata
		WHERE startDT < @cutoff;

		SET @deletedMetadata_count = @@ROWCOUNT;

        COMMIT TRANSACTION;

        -- Log success
        SET @status_msg = CONCAT('Deleted ', @deleted_count, ' FileTable records older than 16 months (startDT < ', FORMAT(@cutoff, 'yyyy-MM-dd'), ').');
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
        VALUES (@Run_id, 5, @status_msg, DATEDIFF(ms, @start, GETDATE()));
		SET @status_msg = CONCAT('Deleted ', @deletedMetadata_count, ' Metadata records older than 16 months (startDT < ', FORMAT(@cutoff, 'yyyy-MM-dd'), ').');
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
        VALUES (@Run_id, 8, @status_msg, DATEDIFF(ms, @start, GETDATE()));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Log error
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)
        VALUES (@Run_id, 4, CONCAT('Filetable Retention Error: ', ERROR_MESSAGE(), ' (Line ', ERROR_LINE(), ')'), DATEDIFF(ms, @start, GETDATE()));
        THROW;
    END CATCH;
	BEGIN TRY
        BEGIN TRANSACTION;

        -- Delete FileTable records older than 16 months
        
		
		COMMIT TRANSACTION;

        -- Log success
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Log error
        
    END CATCH;
END
GO
