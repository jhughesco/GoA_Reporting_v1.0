USE GADATA_RPT;
GO

CREATE TABLE dbo.ft_Maint_Log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    Run_id INT NULL,
    operation_time DATETIME2(3) DEFAULT SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time',
    details NVARCHAR(500) NULL,
    duration_ms INT NULL
);

-- Modify log table to use FK
--ALTER TABLE dbo.ft_Maint_Log
--ADD operation_type_id TINYINT NOT NULL DEFAULT 4
--    CONSTRAINT FK_ft_Maint_Log_OperationTypes
--    REFERENCES dbo.ft_Maint_OperationTypes(operation_type_id);
