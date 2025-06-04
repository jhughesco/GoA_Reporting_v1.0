USE [GADATA_RPT]
GO

CREATE TABLE [dbo].[ft_Process_State](
	[process_name] [nvarchar](50) NOT NULL,
	[last_processed_time] [datetime] NULL,
	[last_run_id] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[process_name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO