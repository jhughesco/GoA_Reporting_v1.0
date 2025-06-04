USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_2.Load_2.Metadata_Variables]
	    @Run_id INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

	DECLARE @last_run_id INT;
    DECLARE @start DATETIME = GETDATE();

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

    BEGIN TRY
        BEGIN TRANSACTION;

		DECLARE @merge_results TABLE (action NVARCHAR(10));

		MERGE dbo.ft_Metadata_Variables AS target
		USING (
			SELECT
				m.Run_id,
				m.stream_id,
				m.startDate,
				m.job_number,
				SUBSTRING(
					s.value,
					CHARINDEX('''', s.value) + 1,
					CHARINDEX('''', s.value, CHARINDEX('''', s.value) + 1) - CHARINDEX('''', s.value) - 1
				) AS variable_name,
				CASE
					WHEN CHARINDEX('with value ''', s.value) > 0
							AND (LEN(s.value) - CHARINDEX('''', REVERSE(s.value)) + 1) > (CHARINDEX('with value ''', s.value) + LEN('with value '''))
					THEN SUBSTRING(
							s.value,
							CHARINDEX('with value ''', s.value) + LEN('with value '''),
							(LEN(s.value) - CHARINDEX('''', REVERSE(s.value)) + 1) - (CHARINDEX('with value ''', s.value) + LEN('with value '''))
							)
					ELSE NULL
				END AS variable_value,
				m.projName,
				m.projPath,
				GETDATE() AS extracted_at
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
			  AND f.name LIKE '%.log%' AND f.name NOT LIKE '%error%' AND f.name NOT LIKE '%-1%'
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
			VALUES (source.Run_id, source.stream_id, source.startDate, source.job_number, source.variable_name, source.variable_value, source.projName, source.projPath, source.extracted_at)
		OUTPUT $action INTO @merge_results;

		DECLARE @inserted_count INT = (SELECT COUNT(*) FROM @merge_results WHERE action = 'INSERT');
		DECLARE @updated_count  INT = (SELECT COUNT(*) FROM @merge_results WHERE action = 'UPDATE');
		DECLARE @tried_count    INT = (
			SELECT COUNT(*) FROM (
				-- Same source SELECT as above, but just count rows
				SELECT
					m.stream_id,
					m.startDate,
					SUBSTRING(
						s.value,
						CHARINDEX('''', s.value) + 1,
						CHARINDEX('''', s.value, CHARINDEX('''', s.value) + 1) - CHARINDEX('''', s.value) - 1
					) AS variable_name
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
				  AND f.name LIKE '%.log%' AND f.name NOT LIKE '%error%' AND f.name NOT LIKE '%-1%'
			) src
		);
		DECLARE @skipped_count INT = @tried_count - (@inserted_count + @updated_count);

		-- Log results
		INSERT INTO dbo.ft_Maint_Log (Run_id, operation_type_id, details, duration_ms)
		VALUES (
			@Run_id, 
			12, 
			CONCAT(
				'Inserted ', @inserted_count, 
				', Updated ', @updated_count, 
				', Skipped ', @skipped_count, 
				' Metadata Variable records.'
			), 
			DATEDIFF(MS, @start, GETDATE())
		);


        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO dbo.ft_Maint_Log (Run_id, operation_type_id, details, duration_ms)
        VALUES (@Run_id, 4, CONCAT('Metadata Variables Load Error: ', ERROR_MESSAGE(), ' (Line ', ERROR_LINE(), ')'), DATEDIFF(MS, @start, GETDATE()));
        THROW;
    END CATCH;
END
GO

