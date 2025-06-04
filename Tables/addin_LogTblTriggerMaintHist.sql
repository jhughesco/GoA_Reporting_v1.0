USE [GADATA_RPT]
GO

CREATE TABLE [dbo].[addin_LogTblTriggerMaintHist](
	[ActionID] [int] IDENTITY(999999,1) NOT NULL,
	[TableAffected] [varchar](255) NULL,
	[RowsAffected] [int] NULL,
	[Action] [varchar](max) NULL,
	[ActionDT] [datetime] NULL,
	[ErrorCode] [int] NULL,
	[ErroMessage] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO