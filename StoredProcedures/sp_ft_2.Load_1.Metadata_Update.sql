USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_2.Load_1.Metadata_Update]
	    @Run_id INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start DATETIME = GETDATE();
    DECLARE @inserted_count INT = 0;
    DECLARE @last_processed_time DATETIME2(3);

    -- Get last processed time from state table
    SELECT @last_processed_time = last_processed_time  
    FROM dbo.[ft_Process_State]  
    WHERE process_name = 'MetadataUpdate';  

    -- If first run, default to oldest possible date
    IF @last_processed_time IS NULL  
        SET @last_processed_time = '2024-01-01';
		
    IF @Run_id = 0  
        SELECT @Run_id = ABS(CHECKSUM(NEWID())) % 999999999;  

    BEGIN TRY  
        BEGIN TRANSACTION;  

        -- Insert new metadata rows for files created since last processed time
        INSERT INTO dbo.ft_Metadata (
            metadata_date, stream_id, startDT, job_number, projName, projPath, submittedBy, submittedFrom, Run_id
        )
        SELECT
            f.creation_time AS metadata_date,
            f.stream_id,
            TRY_CAST(SUBSTRING(
                MAX(CASE WHEN s.ordinal = 1 THEN s.value END),
                NULLIF(CHARINDEX(':', MAX(CASE WHEN s.ordinal = 1 THEN s.value END), 40), 0) + 2,
                1000
            ) AS DATETIME2(3)) AS startDT,
            TRY_CAST(SUBSTRING(
                MAX(CASE WHEN s.ordinal = 2 THEN s.value END),
                NULLIF(CHARINDEX(':', MAX(CASE WHEN s.ordinal = 2 THEN s.value END), 40), 0) + 2,
                1000
            ) AS BIGINT) AS job_number,
			TRIM(RIGHT(
       			MAX(CASE WHEN s.ordinal = 3 THEN s.value END),
       			CHARINDEX('/', REVERSE(MAX(CASE WHEN s.ordinal = 3 THEN s.value END))) - 1 )
			) AS projName,
			TRIM(LEFT(
				LTRIM(SUBSTRING(
					MAX(CASE WHEN s.ordinal = 3 THEN s.value END),
					CHARINDEX('Project Name: ', MAX(CASE WHEN s.ordinal = 3 THEN s.value END)) + LEN('Project Name: '),
					LEN(MAX(CASE WHEN s.ordinal = 3 THEN s.value END))
				)),
				LEN(LTRIM(SUBSTRING(
					MAX(CASE WHEN s.ordinal = 3 THEN s.value END),
					CHARINDEX('Project Name: ', MAX(CASE WHEN s.ordinal = 3 THEN s.value END)) + LEN('Project Name: '),
					LEN(MAX(CASE WHEN s.ordinal = 3 THEN s.value END))
				)))
				  - CHARINDEX('/', REVERSE(LTRIM(SUBSTRING(
						MAX(CASE WHEN s.ordinal = 3 THEN s.value END),
						CHARINDEX('Project Name: ', MAX(CASE WHEN s.ordinal = 3 THEN s.value END)) + LEN('Project Name: '),
						LEN(MAX(CASE WHEN s.ordinal = 3 THEN s.value END))
					)))
				)
			)) AS projPath,
            TRIM(SUBSTRING(
                MAX(CASE WHEN s.ordinal = 4 THEN s.value END),
                NULLIF(CHARINDEX(':', MAX(CASE WHEN s.ordinal = 4 THEN s.value END), 40), 0) + 2,
                1000
            )) AS submittedBy,
            TRIM(SUBSTRING(
                MAX(CASE WHEN s.ordinal = 5 THEN s.value END),
                NULLIF(CHARINDEX(':', MAX(CASE WHEN s.ordinal = 5 THEN s.value END), 40), 0) + 2,
                1000
            )) AS submittedFrom,
			@Run_id
        FROM dbo.ft_JobLogsFT f
        CROSS APPLY (
            SELECT ordinal, value
            FROM STRING_SPLIT(
                REPLACE(CONVERT(VARCHAR(MAX), f.file_stream), CHAR(13) + CHAR(10), CHAR(10)),
                CHAR(10),
                1
            )
            WHERE RTRIM(value) <> ''
              AND ordinal <= 5
        ) s
        WHERE f.name LIKE '%.log%' AND f.name NOT LIKE '%error%' AND f.name NOT LIKE '%-1%'
          AND f.creation_time > @last_processed_time
          AND NOT EXISTS (
                SELECT 1 FROM dbo.ft_Metadata m WHERE m.stream_id = f.stream_id
          )
        GROUP BY f.stream_id, f.creation_time, f.file_stream
		OPTION (RECOMPILE);

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
                    'MetadataUpdate' AS process_name,
                    MAX(f.creation_time) AS last_processed_time
				FROM dbo.ft_JobLogsFT f
                WHERE f.creation_time > @last_processed_time AND f.name NOT LIKE '%error%' AND f.name NOT LIKE '%-1%'
            ) AS source  
            ON target.process_name = source.process_name  
            WHEN MATCHED THEN  
                UPDATE SET last_processed_time = source.last_processed_time, last_run_id = @Run_id
            WHEN NOT MATCHED THEN  
                INSERT (process_name, last_processed_time, last_run_id)
                VALUES (source.process_name, source.last_processed_time, @Run_id);  
        END  

		 -- Log results  
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)  
        VALUES (@Run_id, 3, CONCAT(  
            'Inserted ', @inserted_count, ' new Metadata records.'  
        ), DATEDIFF(MS, @start, GETDATE()));

        COMMIT TRANSACTION;  
    END TRY  
    BEGIN CATCH  
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;  
        PRINT 'Error: ' + ERROR_MESSAGE();
		PRINT 'Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
        INSERT INTO dbo.[ft_Maint_Log] (Run_id, operation_type_id, details, duration_ms)  
        VALUES (@Run_id, 4, CONCAT('Metadata Update Error: ', ERROR_MESSAGE(), ' (Line ', ERROR_LINE(), ')'), DATEDIFF(MS, @start, GETDATE()));
        THROW;  
    END CATCH;  
END
GO
