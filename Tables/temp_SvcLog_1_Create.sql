USE [GADATA_RPT]
GO

/****** Object:  Table [dbo].[temp_SvcLog_1_Create]    Script Date: 3/31/2025 11:55:16 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[temp_SvcLog_1_Create](
	[TableNamePrefix] [nvarchar](255) NULL,
	[TableName] [nvarchar](255) NULL,
	[PartitionFunctionName] [nvarchar](100) NULL,
	[PartitionSchemeName] [nvarchar](100) NULL,
	[SourceServer] [nvarchar](128) NULL,
	[SourceDatabase] [nvarchar](128) NULL,
	[SourceTableName] [nvarchar](100) NULL,
	[PartitionColumn] [nvarchar](100) NULL,
	[FastRetentionPeriodDays] [int] NULL,
	[ArchiveRetentionPeriodDays] [int] NULL,
	[IDColumnName] [nvarchar](128) NULL,
	[IDColumnName2] [nvarchar](128) NULL,
	[SourceStartDT] [datetime] NULL,
	[SourceEndDT] [datetime] NULL,
	[TableDistroLvl] [int] NULL,
	[WeekTbl] [int] NULL
) ON [PRIMARY]
GO


