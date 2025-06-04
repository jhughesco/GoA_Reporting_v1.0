USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_2.Load_2.Snapshot_Incremental]  
    @Run_id INT = 0 OUTPUT  
AS  
BEGIN  
    SET NOCOUNT ON;  
    DECLARE @start DATETIME = GETDATE();  
    DECLARE @inserted_count INT = 0;  
    DECLARE @last_processed_time DATETIME;  

    IF @Run_id = 0  
    BEGIN  
        SELECT @Run_id = ABS(CHECKSUM(NewId())) % 999999999;  
    END  

    -- Get last processed time from state table
    SELECT @last_processed_time = last_processed_time  
    FROM dbo.[ft_Process_State]  
    WHERE process_name = 'MetadataSnapshot';  

    -- If first run, default to oldest possible date
    IF @last_processed_time IS NULL  
        --SET @last_processed_time = '1900-01-01';  
		SET @last_processed_time = '2025-05-07';  

    BEGIN TRY  
        BEGIN TRANSACTION;  

        -- Insert new records using FileTable's creation_time
        INSERT INTO dbo.[ft_Metadata_Snapshot] (  
            snapshot_date,  
            stream_id,  
            startDT,  
            job_number,  
            projName,  
            submittedBy,  
            submittedFrom  
        )  
        SELECT  
            GETDATE(),  
            v.stream_id,  
            v.startDT,  
            v.job_number,  
            v.projName,  
            v.submittedBy,  
            v.submittedFrom  
        FROM vw_ft_TypedMetadata v  
        INNER JOIN dbo.[ft_JobLogsFT] f -- Replace with your FileTable name
            ON v.stream_id = f.stream_id  
        WHERE f.creation_time > @last_processed_time
			AND NOT EXISTS (
				SELECT 1 
				FROM dbo.[ft_Metadata_Snapshot] s 
				WHERE s.stream_id = v.stream_id AND s.startDT = v.startDT
			);  

        SET @inserted_count = @@ROWCOUNT;  

		-- Debug: Print counts
        PRINT 'Inserted ' + CAST(@inserted_count AS VARCHAR(10)) + ' records.';
        PRINT 'Run_id: ' + CAST(@Run_id AS VARCHAR(10));

        -- Update state with newest creation_time
        IF @inserted_count > 0  
        BEGIN  
            MERGE dbo.[ft_Process_State] AS target  
            USING (  
                SELECT  
                    'MetadataSnapshot' AS process_name,  
                    MAX(f.creation_time) AS last_processed_time  
                FROM dbo.[ft_JobLogsFT] f  
                INNER JOIN vw_ft_TypedMetadata v  
                    ON f.stream_id = v.stream_id  
            ) AS source  
            ON target.process_name = source.process_name  
            WHEN MATCHED THEN  
                UPDATE SET last_processed_time = source.last_processed_time  
            WHEN NOT MATCHED THEN  
                INSERT (process_name, last_processed_time)  
                VALUES (source.process_name, source.last_processed_time);  
        END

        -- Log results  
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)  
        VALUES (@Run_id, 3, CONCAT(  
            'Inserted ', @inserted_count, ' new records.'  
        ), DATEDIFF(MS, @start, GETDATE()));
		
		COMMIT TRANSACTION;

    END TRY  
    BEGIN CATCH  
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;  
        -- Log error  
        PRINT 'Error in CATCH: ' + ERROR_MESSAGE();
        PRINT 'Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)  
        VALUES (@Run_id, 4, CONCAT('Error: ', ERROR_MESSAGE(), ' (Line ', ERROR_LINE(), ')'), DATEDIFF(MS, @start, GETDATE()));  
        THROW;  
    END CATCH;  
END  
