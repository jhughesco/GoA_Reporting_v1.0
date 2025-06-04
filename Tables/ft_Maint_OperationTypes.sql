USE GADATA_RPT;
GO

-- Create log type table
CREATE TABLE dbo.ft_Maint_OperationTypes (
    operation_type_id TINYINT PRIMARY KEY,
    operation_name VARCHAR(50) NOT NULL UNIQUE
);

-- Insert allowed operations
INSERT INTO dbo.ft_Maint_OperationTypes
VALUES 
    (1, 'IndexPopulate'),
    (2, 'GarbageCollection'),
    (3, 'Snapshot'),
    (4, 'Error');

-- Modify log table to use FK
ALTER TABLE dbo.ft_Maint_Log
ADD operation_type_id TINYINT NOT NULL DEFAULT 4
    CONSTRAINT FK_ft_Maint_Log_OperationTypes
    REFERENCES dbo.ft_Maint_OperationTypes(operation_type_id);