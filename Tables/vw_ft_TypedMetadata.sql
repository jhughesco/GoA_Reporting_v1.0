USE [GADATA_RPT]
GO

CREATE OR ALTER VIEW dbo.vw_ft_TypedMetadata
AS
SELECT
    stream_id,
    name,
    TRY_CAST(creation_time AS DATETIME2(3)) AS creation_time,
    TRY_CAST(startDT AS DATETIME2(3)) AS startDT,
    TRY_CAST(job_number AS BIGINT) AS job_number,
    TRIM(projName) AS projName,
    TRIM(submittedBy) AS submittedBy,
    TRIM(submittedFrom) AS submittedFrom
FROM vw_ft_Metadata;
GO