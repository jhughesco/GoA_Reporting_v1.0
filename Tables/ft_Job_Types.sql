USE GADATA_RPT
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
CREATE TABLE dbo.ft_JobTypes
	(
	type_id varchar(2) NOT NULL,
	type_name varchar(50) NOT NULL
	)  ON [PRIMARY]
GO

ALTER TABLE dbo.ft_JobTypes ADD CONSTRAINT
	PKft_JobTypes PRIMARY KEY CLUSTERED 
	(
	type_id
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO

ALTER TABLE dbo.ft_JobTypes SET (LOCK_ESCALATION = TABLE)
GO
COMMIT

BEGIN TRANSACTION
GO
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('1','Admin UI')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('2','Scheduler')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('3','Trigger')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('4','Monitor')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('5','API-GACMD')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('7','Secure Form')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('9','Secure Form REST')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('A','Agent Monitor')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('L','Service Level Agreement')
INSERT INTO [dbo].[ft_JobTypes] ([type_id],[type_name]) VALUES ('S','Agent Scheduler')

GO
COMMIT



