USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_1.Maint_4.Snapshot_Retention]  
    @retention_days INT = 30,  
    @Run_id INT = 0 OUTPUT  
AS  
BEGIN  
    SET NOCOUNT ON;  
    DECLARE @start DATETIME = GETDATE();  
    DECLARE @deleted_count INT = 0;  
    DECLARE @retention_cutoff DATE = DATEADD(DAY, -@retention_days, GETDATE());  

    IF @Run_id = 0  
    BEGIN  
        SELECT @Run_id = ABS(CHECKSUM(NewId())) % 999999999;  
    END  

    BEGIN TRY  
        BEGIN TRANSACTION;  

        -- Delete records older than retention cutoff  
        DELETE FROM dbo.[ft_Metadata_Snapshot]  
        WHERE startDT < @retention_cutoff;  

        SET @deleted_count = @@ROWCOUNT;  

        COMMIT TRANSACTION;  

        -- Log results  
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)  
        VALUES (@Run_id, 3, CONCAT(  
            'Deleted ', @deleted_count,  
            ' records with startDT < ', FORMAT(@retention_cutoff, 'yyyy-MM-dd')  
        ), DATEDIFF(MS, @start, GETDATE()));  
    END TRY  
    BEGIN CATCH  
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;  
        -- Log error  
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)  
        VALUES (@Run_id, 4, CONCAT(  
            'Error: ', ERROR_MESSAGE(),  
            ' (Line ', ERROR_LINE(), ')'  
        ), DATEDIFF(MS, @start, GETDATE()));  
        THROW;  
    END CATCH;  
END  
