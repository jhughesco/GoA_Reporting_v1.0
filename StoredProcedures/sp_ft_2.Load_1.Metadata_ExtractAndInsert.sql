USE GADATA_RPT;
GO

CREATE OR ALTER PROCEDURE dbo.[sp_ft_2.Load_1.Metadata_ExtractAndInsert]
    @stream_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @name NVARCHAR(255), @creation_time DATETIME2(3), @file_stream VARBINARY(MAX);

    SELECT @name = name, @creation_time = creation_time, @file_stream = file_stream
    FROM dbo.ft_JobLogsFT
    WHERE stream_id = @stream_id;

    -- Only process non-error files
    IF @name NOT LIKE '%error%'
    BEGIN
        -- Parse the first 5 lines for metadata
        DECLARE @text NVARCHAR(MAX) = CONVERT(NVARCHAR(MAX), @file_stream);
        DECLARE @lines NVARCHAR(MAX) = REPLACE(@text, CHAR(13)+CHAR(10), CHAR(10));
        DECLARE @startDT DATETIME2(3), @job_number BIGINT, @projName NVARCHAR(256), @submittedBy NVARCHAR(256), @submittedFrom NVARCHAR(256);

        -- Use a table variable to split lines
        DECLARE @tbl TABLE (ordinal INT IDENTITY(1,1), value NVARCHAR(1000));
        INSERT INTO @tbl (value)
        SELECT value FROM STRING_SPLIT(@lines, CHAR(10)) WHERE RTRIM(value) <> '';

        -- Extract metadata (assumes colon after label)
        SELECT
            @startDT = TRY_CAST(SUBSTRING(value, CHARINDEX(':', value, 40) + 2, 1000) AS DATETIME2(3))
        FROM @tbl WHERE ordinal = 1;

        SELECT
            @job_number = TRY_CAST(SUBSTRING(value, CHARINDEX(':', value, 40) + 2, 1000) AS BIGINT)
        FROM @tbl WHERE ordinal = 2;

        SELECT
            @projName = LTRIM(RTRIM(SUBSTRING(value, CHARINDEX(':', value, 40) + 2, 1000)))
        FROM @tbl WHERE ordinal = 3;

        SELECT
            @submittedBy = LTRIM(RTRIM(SUBSTRING(value, CHARINDEX(':', value, 40) + 2, 1000)))
        FROM @tbl WHERE ordinal = 4;

        SELECT
            @submittedFrom = LTRIM(RTRIM(SUBSTRING(value, CHARINDEX(':', value, 40) + 2, 1000)))
        FROM @tbl WHERE ordinal = 5;

        INSERT INTO dbo.ft_Metadata (
            metadata_date, stream_id, startDT, job_number, projName, submittedBy, submittedFrom
        )
        VALUES (
            @creation_time, @stream_id, @startDT, @job_number, @projName, @submittedBy, @submittedFrom
        );
    END
END
GO
