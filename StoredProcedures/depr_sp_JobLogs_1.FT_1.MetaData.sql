USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_JobLogs_1.MetaData.Master]
    @stream_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @creation_time DATETIME2(3),
        @startDT DATETIME2(3),
        @job_number BIGINT,
        @projName NVARCHAR(200),
        @submittedBy NVARCHAR(100),
        @submittedFrom NVARCHAR(100),
        @file_content NVARCHAR(MAX),
        @crlf CHAR(2) = CHAR(13) + CHAR(10),
        @lineCount INT;

    BEGIN TRY
        -- Get file content from FileTable
        SELECT @file_content = CONVERT(VARCHAR(MAX), file_stream), 
               @creation_time = creation_time
        FROM [ft_JobLogsFT]
        WHERE stream_id = @stream_id;

        PRINT 'File content retrieved: ' + LEFT(@file_content, 500); -- Print first 500 characters for verification
        PRINT 'Creation time: ' + CONVERT(NVARCHAR, @creation_time, 121);


        -- Extract first 5 lines
        DECLARE @lines TABLE (LineNumber INT IDENTITY(1,1), LineText NVARCHAR(MAX));
        
        INSERT INTO @lines (LineText)
        SELECT value
        FROM STRING_SPLIT(REPLACE(@file_content, @crlf, CHAR(10)), CHAR(10))
        WHERE RTRIM(value) <> ''
        ORDER BY (SELECT NULL)
        OFFSET 0 ROWS FETCH FIRST 5 ROWS ONLY;

        
		
		-- Get line count
        SELECT @lineCount = COUNT(*) FROM @lines;
        PRINT 'Lines extracted: ' + CONVERT(NVARCHAR(10), @lineCount);

        -- Parse lines
		SELECT @lineCount = COUNT(*) FROM @lines;
        PRINT 'Lines extracted: ' + CONVERT(NVARCHAR(10), @lineCount);

        -- Parse lines using individual queries
        SELECT @startDT = TRY_CAST(
            SUBSTRING(LineText, NULLIF(CHARINDEX(':', LineText, 40),0) + 2, 1000) 
            AS DATETIME2(3))
        FROM @lines WHERE LineNumber = 1;

        SELECT @job_number = TRY_CAST(
            SUBSTRING(LineText, NULLIF(CHARINDEX(':', LineText, 40),0) + 2, 1000) 
            AS BIGINT)
        FROM @lines WHERE LineNumber = 2;

        SELECT @projName = SUBSTRING(
            LineText, NULLIF(CHARINDEX(':', LineText, 40),0) + 2, 1000)
        FROM @lines WHERE LineNumber = 3;

        SELECT @submittedBy = SUBSTRING(
            LineText, NULLIF(CHARINDEX(':', LineText, 40),0) + 2, 1000)
        FROM @lines WHERE LineNumber = 4;

        SELECT @submittedFrom = SUBSTRING(
            LineText, NULLIF(CHARINDEX(':', LineText, 40),0) + 2, 1000)
        FROM @lines WHERE LineNumber = 5;

        PRINT 'Parsed values:';
        PRINT 'StartDT: ' + CONVERT(NVARCHAR, @startDT, 121);
        PRINT 'Job Number: ' + CONVERT(NVARCHAR, @job_number);
        PRINT 'Project Name: ' + @projName;
        PRINT 'Submitted By: ' + @submittedBy;
        PRINT 'Submitted From: ' + @submittedFrom;

        -- Insert into metadata table
        INSERT INTO log_job_metadata (
            jf_type_id, stream_id, creation_time, 
            startDT, job_number, projName, submittedBy, submittedFrom
        )
        SELECT
            (SELECT jf_type_id FROM [log_job_type] WHERE jf_type_name LIKE '%' + @submittedFrom + '%'),  -- Replace with actual type ID logic
            @stream_id,
            CAST(@creation_time as DATETIME2(3)),
            CAST(@startDT as DATETIME2(3)),
            CAST(@job_number as BIGINT),
            TRIM(@projName),
            TRIM(@submittedBy),
            TRIM(@submittedFrom);

        PRINT 'Metadata inserted successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;
GO
