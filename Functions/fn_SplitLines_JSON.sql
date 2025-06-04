USE GADATA_RPT;
GO
CREATE OR ALTER FUNCTION dbo.fn_SplitLines_JSON(@Text NVARCHAR(MAX))
RETURNS TABLE
AS
RETURN
(
    SELECT [key] + 1 AS ordinal, [value]
    FROM OPENJSON(
        CONCAT(
            '["',
            REPLACE(
                REPLACE(
                    REPLACE(@Text, '\', '\\'),   -- Escape backslash first
                    '"', '\"'                   -- Then escape double quote
                ),
                CHAR(10), '","'
            ),
            '"]'
        )
    )
);


