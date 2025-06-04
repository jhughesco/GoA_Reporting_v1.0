USE [GADATA_RPT]
GO

CREATE OR ALTER VIEW dbo.vw_ft_MetadataStartDT
AS
SELECT
    f.stream_id,
    f.name,
    f.creation_time,
    TRY_CAST(SUBSTRING(
        MAX(CASE WHEN s.ordinal = 1 THEN s.value END),
        NULLIF(CHARINDEX(':', MAX(CASE WHEN s.ordinal = 1 THEN s.value END), 40),0)+2,
        1000
    ) AS DATETIME2(3)) AS startDT
FROM ft_JobLogsFT f
CROSS APPLY (
    SELECT ordinal, value
    FROM STRING_SPLIT(
        REPLACE(CONVERT(VARCHAR(MAX),f.file_stream),CHAR(13)+CHAR(10),CHAR(10)),
        CHAR(10),
        1  -- Enable ordinal flag
    )
    WHERE RTRIM(value) <> ''
    AND ordinal <= 5
) s
WHERE f.name NOT LIKE '%error%'
GROUP BY f.stream_id, f.name, f.creation_time;
GO