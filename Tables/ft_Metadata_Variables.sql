USE [GADATA_RPT]
GO

CREATE TABLE ft_Metadata_Variables (
    Run_id int NULL,
	stream_id UNIQUEIDENTIFIER NOT NULL,
    startDate DATE NOT NULL,
	job_number bigint NULL,
    variable_name NVARCHAR(255) NOT NULL,
    variable_value NVARCHAR(4000) NULL,
    projName nvarchar(256) NULL,
	projPath nvarchar(512) NULL,
	extracted_at DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT PK_MetadataVariables PRIMARY KEY (stream_id, startDate, variable_name),
    CONSTRAINT FK_Metadata_Stream FOREIGN KEY (stream_id, startDate) 
        REFERENCES ft_Metadata(stream_id, startDate)
);

-- Partition-aligned index for common queries
CREATE NONCLUSTERED INDEX IX_ProjName_Value
ON ft_Metadata_Variables(projName, variable_value)
INCLUDE (variable_name, extracted_at)
ON [ps_metadata_daily](startDate);