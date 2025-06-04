USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_2.Load_2.Metadata_Variables2]
    @Run_id INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @last_run_id INT;
    DECLARE @start DATETIME = GETDATE();
    DECLARE @FileName NVARCHAR(255), @LogLine NVARCHAR(MAX), @StreamID UNIQUEIDENTIFIER;
    DECLARE @ErrorDetails NVARCHAR(MAX);

    -- Get the most recent Run_id from ft_Process_State
    SELECT @last_run_id = 497804745
    FROM dbo.ft_Process_State
    WHERE process_name = 'MetadataUpdate';

    IF @last_run_id IS NULL
    BEGIN
        RAISERROR('No Run_id found in ft_Process_State for MetadataUpdate.', 16, 1);
        RETURN;
    END

    IF @Run_id = 0  
        SELECT @Run_id = ABS(CHECKSUM(NEWID())) % 999999999;

    -- Cursor to process each log line individually
    DECLARE log_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT 
        f.name AS FileName,
        s.value AS LogLine,
        m.stream_id
    FROM dbo.ft_Metadata m
    INNER JOIN dbo.ft_JobLogsFT f
        ON m.stream_id = f.stream_id
    CROSS APPLY (
        SELECT ordinal, value
        FROM STRING_SPLIT(
            REPLACE(CONVERT(VARCHAR(MAX), f.file_stream), CHAR(13) + CHAR(10), CHAR(10)),
            CHAR(10),
            1
        )
        WHERE RTRIM(value) <> ''
          AND ordinal BETWEEN 6 AND 40
          AND value LIKE '%Overriding variable%'
    ) s
    WHERE m.Run_id = @last_run_id
      AND f.name LIKE '%.log%' AND f.name NOT LIKE '%error%' AND f.name NOT LIKE '%-1%';

    OPEN log_cursor;
    FETCH NEXT FROM log_cursor INTO @FileName, @LogLine, @StreamID;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            MERGE dbo.ft_Metadata_Variables AS target
            USING (
                SELECT
                    m.Run_id,
                    m.stream_id,
                    m.startDate,
                    m.job_number,
                    SUBSTRING(
                        @LogLine,
                        CHARINDEX('''', @LogLine) + 1,
                        CHARINDEX('''', @LogLine, CHARINDEX('''', @LogLine) + 1) - CHARINDEX('''', @LogLine) - 1
                    ) AS variable_name,
                    CASE 
						WHEN CHARINDEX('with value ''', @LogLine) > 0 
							 AND (LEN(@LogLine) - CHARINDEX('''', REVERSE(@LogLine)) + 1) > (CHARINDEX('with value ''', @LogLine) + LEN('with value '''))
						THEN SUBSTRING(
								@LogLine,
								CHARINDEX('with value ''', @LogLine) + LEN('with value '''),
								(LEN(@LogLine) - CHARINDEX('''', REVERSE(@LogLine)) + 1) - (CHARINDEX('with value ''', @LogLine) + LEN('with value '''))
							 )
						ELSE NULL
					END AS variable_value
,
                    m.projName,
                    m.projPath,
                    GETDATE() AS extracted_at
                FROM dbo.ft_Metadata m
                WHERE m.stream_id = @StreamID
            ) AS source
            ON target.stream_id = source.stream_id
                AND target.startDate = source.startDate
                AND target.variable_name = source.variable_name
            WHEN MATCHED THEN
                UPDATE SET 
                    target.variable_value = source.variable_value,
                    target.job_number = source.job_number,
                    target.projName = source.projName,
                    target.projPath = source.projPath,
                    target.extracted_at = source.extracted_at,
                    target.Run_id = source.Run_id
            WHEN NOT MATCHED THEN
                INSERT (Run_id, stream_id, startDate, job_number, variable_name, variable_value, projName, projPath, extracted_at)
                VALUES (source.Run_id, source.stream_id, source.startDate, source.job_number, source.variable_name, source.variable_value, source.projName, source.projPath, source.extracted_at);

            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            
            SET @ErrorDetails = CONCAT(
                'Error parsing line in file: ', @FileName,
                ' | Line: ', @LogLine,
                ' | Error: ', ERROR_MESSAGE()
            );
            
            INSERT INTO dbo.ft_Maint_Log (Run_id, operation_type_id, details, duration_ms)
            VALUES (@Run_id, 4, @ErrorDetails, DATEDIFF(MS, @start, GETDATE()));
        END CATCH

        FETCH NEXT FROM log_cursor INTO @FileName, @LogLine, @StreamID;
    END

    CLOSE log_cursor;
    DEALLOCATE log_cursor;

    -- Log overall completion
    INSERT INTO dbo.ft_Maint_Log (Run_id, operation_type_id, details, duration_ms)
    VALUES (@Run_id, 12, 'Metadata Variables load completed with detailed error logging', DATEDIFF(MS, @start, GETDATE()));
END
GO

