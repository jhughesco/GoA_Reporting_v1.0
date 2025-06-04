USE [GADATA_RPT]
GO

/****** Object:  Table [dbo].[temp_SvcLog_2_Load]    Script Date: 3/31/2025 11:55:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[temp_SvcLog_2_Load](
	[TableNamePrefix] [nvarchar](255) NULL,
	[TableName] [nvarchar](255) NULL,
	[SourceServer] [nvarchar](128) NULL,
	[SourceDatabase] [nvarchar](128) NULL,
	[SourceTableName] [nvarchar](100) NULL,
	[SourceStartDT] [datetime] NULL,
	[SourceEndDT] [datetime] NULL,
	[IDColumnName] [nvarchar](128) NULL,
	[IDColumnName2] [nvarchar](128) NULL,
	[PartitionColumn] [nvarchar](100) NULL,
	[TableDistroLvl] [int] NULL,
	[WeekTbl] [int] NULL
) ON [PRIMARY]
GO


