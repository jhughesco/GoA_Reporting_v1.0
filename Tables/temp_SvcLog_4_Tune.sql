USE [GADATA_RPT]
GO

/****** Object:  Table [dbo].[temp_SvcLog_4_Tune]    Script Date: 3/31/2025 11:55:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[temp_SvcLog_4_Tune](
	[table_name] [nvarchar](255) NULL,
	[index_name] [nvarchar](255) NULL,
	[fragmentation_percent] [float] NULL,
	[page_fill_percent] [float] NULL,
	[type_desc] [nvarchar](20) NULL,
	[FragFlg] int NULL,
	[PageFillFlg] int NULL
) ON [PRIMARY]
GO


