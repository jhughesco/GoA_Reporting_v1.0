USE [GADATA_RPT]
GO

CREATE TABLE [dbo].[ft_Metadata](
	[Run_id] [int] NULL,
	[metadata_date] [datetime2](3) NOT NULL,
	[stream_id] [uniqueidentifier] NOT NULL,
	[startDT] [datetime2](3) NOT NULL,
	[job_number] [bigint] NULL,
	[projName] [nvarchar](256) NULL,
	[projPath] [nvarchar](512) NULL,
	[submittedBy] [nvarchar](256) NULL,
	[submittedFrom] [nvarchar](256) NULL,
	[startDate]  AS (CONVERT([date],[startDT])) PERSISTED NOT NULL,
 CONSTRAINT [PK_FT_Metadata] PRIMARY KEY CLUSTERED 
	([startDT] ASC, [stream_id] ASC,[startDate] ASC) 
) ON [ps_metadata_daily]([startDate]);
GO

CREATE NONCLUSTERED INDEX IX_ft_Metadata_stream_id_startDT
ON dbo.ft_Metadata (stream_id,startDT,startDate);

CREATE NONCLUSTERED INDEX IX_ft_Metadata_Date
ON dbo.ft_Metadata (startDT);
GO

CREATE UNIQUE NONCLUSTERED INDEX UQ_ft_Metadata_stream_id_startDate
ON ft_Metadata(stream_id, startDate)
ON [ps_metadata_daily](startDate);


-------------------
USE [GADATA_RPT]
GO

-- 1. Drop the existing Primary Key constraint
ALTER TABLE dbo.ft_Metadata
DROP CONSTRAINT PK_FT_Metadata;
GO

-- 2. Recreate the Primary Key on the partition scheme
ALTER TABLE dbo.ft_Metadata
ADD CONSTRAINT PK_FT_Metadata PRIMARY KEY CLUSTERED
(
    [startDT] ASC,
    [stream_id] ASC,
    [startDate] ASC
) ON [ps_metadata_daily]([startDate]);
GO




