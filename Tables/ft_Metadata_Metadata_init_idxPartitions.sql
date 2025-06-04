-- Set up daily partition boundaries for the last 16 months + 1 future day
DECLARE @start DATE = DATEADD(MONTH, -16, CAST(GETDATE() AS DATE));
DECLARE @end DATE = DATEADD(DAY, 1, CAST(GETDATE() AS DATE)); -- 1 day ahead
DECLARE @sql NVARCHAR(MAX) = N'';
DECLARE @boundaryList NVARCHAR(MAX) = N'';

-- Build the boundary list for RANGE RIGHT (one per day)
WHILE @start <= @end
BEGIN
    SET @boundaryList += '''' + CONVERT(CHAR(10), @start, 120) + ''',';
    SET @start = DATEADD(DAY, 1, @start);
END

SET @boundaryList = LEFT(@boundaryList, LEN(@boundaryList) - 1);

-- Partition function
SET @sql = N'
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = ''pf_metadata_daily'')
    DROP PARTITION FUNCTION pf_metadata_daily;
CREATE PARTITION FUNCTION pf_metadata_daily (DATE)
AS RANGE RIGHT FOR VALUES (' + @boundaryList + N');
';
PRINT @sql;
EXEC sp_executesql @sql;

-- Partition scheme (all to PRIMARY, adjust as needed)
SET @sql = N'
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = ''ps_metadata_daily'')
    DROP PARTITION SCHEME ps_metadata_daily;
CREATE PARTITION SCHEME ps_metadata_daily
AS PARTITION pf_metadata_daily
ALL TO ([PRIMARY]);
';
PRINT @sql;
EXEC sp_executesql @sql;
GO
