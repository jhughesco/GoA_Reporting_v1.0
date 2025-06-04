USE [GADATA_RPT]
GO

BEGIN TRANSACTION
SET QUOTED_IDENTIFIER ON
SET ARITHABORT ON
SET NUMERIC_ROUNDABORT OFF
SET CONCAT_NULL_YIELDS_NULL ON
SET ANSI_NULLS ON
SET ANSI_PADDING ON
SET ANSI_WARNINGS ON
COMMIT
BEGIN TRANSACTION
GO
-- Create FileTable with proper syntax
CREATE TABLE ft_JobLogsFT AS FILETABLE
WITH (
    FILETABLE_DIRECTORY = 'GADATA_RPT_FS_JobLogs',
    FILETABLE_PRIMARY_KEY_CONSTRAINT_NAME = PK_ft_JobLogsFT
)
GO
COMMIT

-- Index to speed up snapshots
CREATE INDEX IX_ft_JobLogsFT_creation_time_stream_id
ON dbo.ft_JobLogsFT (creation_time, stream_id);

CREATE INDEX IX_ft_JobLogsFT_name
ON dbo.ft_JobLogsFT (name);
GO

-- Create FullTEXT index on the file_stream column
CREATE FULLTEXT INDEX ON ft_JobLogsFT (
    file_stream TYPE COLUMN file_type LANGUAGE 1033  -- 1033 = English
)
KEY INDEX PK_ft_JobLogsFT
ON GADATA_RPT_FTCatalog
WITH (
    CHANGE_TRACKING = AUTO,
    STOPLIST = SYSTEM
)
GO

-- GoA Agent service needs runAs  ilab.local/SVC-GoAiLab
-- ilab.local/SVC-GoAiLab need to be added as a SQL Srv User
-- Grant ilab.local/SVC-GoAiLab permissions on FileTable to allow agent to manipulate files on FileTable file share.

