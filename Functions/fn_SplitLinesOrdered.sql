USE GADATA_RPT
GO

CREATE OR ALTER FUNCTION dbo.fn_SplitLinesOrdered(@text NVARCHAR(MAX))
RETURNS TABLE
AS
RETURN
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS LineNumber,
        value AS LineText
    FROM STRING_SPLIT(
        REPLACE(
            REPLACE(@text, CHAR(13) + CHAR(10), CHAR(10)),
            CHAR(13),
            CHAR(10)
        ),
        CHAR(10)
    )
    WHERE RTRIM(value) <> '';
GO