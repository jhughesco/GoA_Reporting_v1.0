USE [GADATA_RPT]
GO

CREATE TABLE [dbo].[addin_SvcLogTableStructures](
	[TableNamePrefix] [nvarchar](100) NOT NULL,
	[TableDefinition] [nvarchar](max) NULL,
	[PrimaryKeyDefinition] [nvarchar](max) NULL,
	[NonClusteredIndexDefinition] [nvarchar](max) NULL,
	[InsertHistoryTriggerDefinition] [nvarchar](max) NULL,
	[DeleteHistoryTriggerDefinition] [nvarchar](max) NULL,
	[ViewDefinition] [nvarchar](max) NULL,
	[FastPartitionFunctionName] [nvarchar](100) NULL,
	[FastPartitionSchemeName] [nvarchar](100) NULL,
	[ArcPartitionFunctionName] [nvarchar](100) NULL,
	[ArcPartitionSchemeName] [nvarchar](100) NULL,
	[PartitionColumn] [nvarchar](100) NULL,
	[FastRetentionPeriodDays] [int] NULL,
	[ArchiveRetentionPeriodDays] [int] NULL,
	[SourceServer] [nvarchar](128) NULL,
	[SourceDatabase] [nvarchar](128) NULL,
	[SourceTableName] [nvarchar](100) NULL,
	[DestinationDatabase] [nvarchar](128) NULL,
	[IDColumnName] [nvarchar](128) NULL,
	[IDColumnName2] [nvarchar](128) NULL,
	[TableDistroLvl] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[TableNamePrefix] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO