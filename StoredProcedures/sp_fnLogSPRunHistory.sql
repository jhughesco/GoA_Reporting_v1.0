USE [GADATA_RPT]
GO
-- ==========================================================================================
--  Author: JHughes
--  Create date: 02/2025
--  Description:  1) Write to DB activity Logs
-- ==========================================================================================

CREATE Procedure dbo.sp_fnLogSPRunHistory
(
    @Run_id INT,
    @TableAffected NVARCHAR(255) = 'NameMe',  -- Default value
    @RowsAffected INT = 0,
    @Action NVARCHAR(255) = 'CommentMe',      -- Default value
    @ErrorCode INT = 0,
	@ErrorMessage NVARCHAR(255) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

	DECLARE @ActionDT DATETIME = GETDATE(); 
	DECLARE @ErrorNumber INT = @ErrorCode;

	-- Log the result into addin_LogSProcInsertHistory
	IF @RowsAffected > 0
	BEGIN
	    SET @ErrorMessage = 'Success!';
		INSERT INTO dbo.addin_Log_SPRunHistory (Run_id, TableAffected, RowsAffected, Action, ActionDT, ErrorCode, ErrorMessage)
	    VALUES (@Run_id, 
			    @TableAffected, 
			    @RowsAffected, 
			    @Action, 
			    @ActionDT,
				@ErrorCode,
				@ErrorMessage);
	END 
	ELSE IF @RowsAffected = 0
	BEGIN
	    SET @ErrorMessage = 'Success! No rows affected.';
		INSERT INTO dbo.addin_Log_SPRunHistory (Run_id, TableAffected, RowsAffected, Action, ActionDT, ErrorCode, ErrorMessage)
			VALUES (@Run_id, 
					@TableAffected, 
					@RowsAffected, 
					@Action, 
					@ActionDT,
					@ErrorCode,
					@ErrorMessage);
	END ELSE
	BEGIN
		INSERT INTO dbo.addin_Log_SPRunHistory (Run_id, TableAffected, RowsAffected, Action, ActionDT, ErrorCode, ErrorMessage)
			VALUES (@Run_id, 
					@TableAffected, 
					@RowsAffected, 
					@Action, 
					@ActionDT,
					@ErrorCode,
					@ErrorMessage);
		
		PRINT 'Error ' + CAST(@ErrorCode AS NVARCHAR(10)) + ' | State ' + @ErrorMessage;
	END;
END;