USE [GADATA_RPT]
GO

/****** Object:  Table [dbo].[temp_SvcLog_3_Purge]    Script Date: 3/31/2025 11:55:45 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[temp_SvcLog_3_Purge](
	[TableNamePrefix] [nvarchar](255) NULL,
	[TableName] [nvarchar](255) NULL,
	[MoveToTableName] [nvarchar](255) NULL,
	[PartitionColumn] [nvarchar](100) NULL,
	[SourceStartDT] [datetime] NULL,
	[SourceEndDT] [datetime] NULL,
	[TableDistroLvl] [int] NULL,
	[FastRetentionPeriodDays] [int] NULL,
	[ArchiveRetentionPeriodDays] [int] NULL,
	[IDColumnName] [nvarchar](128) NULL,
	[WeekTbl] [int] NULL
) ON [PRIMARY]
GO


