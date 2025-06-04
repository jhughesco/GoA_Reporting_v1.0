USE [GADATA_RPT]
GO

CREATE TABLE [dbo].[addin_Log_StatsHistory](
	[ActionID] [int] IDENTITY(999999,1) NOT NULL,
	[ActionDT] [datetime] NULL,
	[Run_id] [bigint] NULL,
	[table_name] [nvarchar](128) NULL,
	[index_name] [sysname] NULL,
	[fragmentation_percent] [float] NULL,
	[page_fill_percent] [float] NULL,
	[type_desc] [nvarchar](60) NULL,
	[FragFlg] [int] NOT NULL,
	[PageFillFlg] [int] NOT NULL,
	[remarks] NVARCHAR(MAX)
) ON [PRIMARY]
GO