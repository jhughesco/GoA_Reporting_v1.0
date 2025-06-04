USE [GADATA_RPT]
GO

-- ==========================================================================================
--  Author: JHughes
--  Create date: 02/2025
--  Description:  1) Calculate script performance.
-- ==========================================================================================

CREATE OR ALTER FUNCTION [dbo].[fn_ScriptPerformance]
(
    @StartTime NVARCHAR(20)
)
RETURNS INT
AS
BEGIN
    DECLARE @ReturnCode INT = 1; -- Assumue success
    --DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @ScriptPerfMsg NVARCHAR(MAX);
    DECLARE @EndTime NVARCHAR(20);
    
    -- Calculate script performance results
    DECLARE @LoadTime INT = DATEDIFF(millisecond, @StartTime, GETDATE());
    
    DECLARE @hr VARCHAR(2);
    DECLARE @min VARCHAR(2);
    DECLARE @sec VARCHAR(2);
    DECLARE @ms VARCHAR(10);
    
    SELECT 
        @hr = CAST(@LoadTime / 3600000 AS VARCHAR(2)), 
        @min = CAST((@LoadTime % 3600000) / 60000 AS VARCHAR(2)), 
        @sec = CAST((@LoadTime % 60000) / 1000 AS VARCHAR(2)), 
        @ms = CAST(@LoadTime % 1000 AS VARCHAR(10)),
        @EndTime = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss');

	SET @ScriptPerfMsg = CONCAT(
		'Start Time: ', @StartTime, CHAR(13),
		'-----------------------------------------------------------', CHAR(13),
		'Source Table/s Replication Runtime: ', CAST(@LoadTime AS NVARCHAR(10)), ' milliseconds', CHAR(13),
		'Runtime Breakdown: ', CHAR(13),
		'Minutes: ', CAST(@min AS NVARCHAR(2)), CHAR(13),
		'Seconds: ', CAST(@sec AS NVARCHAR(2)), CHAR(13),
		'Milliseconds: ', CAST(@ms AS NVARCHAR(10)), CHAR(13),
		'-----------------------------------------------------------', CHAR(13),
		'End Time: ', @EndTime
		);
    RETURN @ScriptPerfMsg;
END;
GO


