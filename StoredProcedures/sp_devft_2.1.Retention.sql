USE [GADATA_RPT_DEV1]
GO

CREATE or ALTER   PROCEDURE [dbo].[sp_JobLogs_1.FT_1.Retention]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Create temp table to store deleted folder paths
    CREATE TABLE #DeletedFolders (
        path_locator HIERARCHYID,
        parent_path_locator HIERARCHYID,
        name NVARCHAR(255)
    );
    
    -- First pass: Delete expired files and capture their parent folders
    DELETE FROM ft_JobLogsFT
    OUTPUT DELETED.parent_path_locator, DELETED.path_locator, DELETED.name
    INTO #DeletedFolders
    WHERE stream_id IN (
        SELECT f.stream_id
        FROM ft_JobLogsFT f
        JOIN vw_JobLogs_TypedMetadata md ON f.stream_id = md.stream_id
        WHERE md.startDT < DATEADD(MONTH, -16, GETDATE())
    );
    
    -- Second pass: Remove empty parent folders
    DECLARE @folderCount INT = 1;
    
    WHILE @folderCount > 0
    BEGIN
        DELETE FROM ft_JobLogsFT
        WHERE is_directory = 1
        AND path_locator IN (
            SELECT f.path_locator
            FROM ft_JobLogsFT f
            WHERE f.is_directory = 1
            AND NOT EXISTS (
                SELECT 1
                FROM ft_JobLogsFT child
                WHERE child.parent_path_locator = f.path_locator
            )
            -- Only folders from our deleted files hierarchy
            AND f.path_locator IN (
                SELECT parent_path_locator
                FROM #DeletedFolders
                WHERE parent_path_locator IS NOT NULL
            )
        );
        
        SET @folderCount = @@ROWCOUNT;
        
        -- Update deleted folders table with newly empty parents
        INSERT INTO #DeletedFolders
        SELECT path_locator, parent_path_locator, name
        FROM ft_JobLogsFT
        WHERE is_directory = 1
        AND path_locator IN (
            SELECT parent_path_locator
            FROM #DeletedFolders
        );
    END;
END;
