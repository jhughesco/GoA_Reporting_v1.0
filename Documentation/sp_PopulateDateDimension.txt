USE [GADATA_RPT]
GO

SET QUOTED_IDENTIFIER ON
GO
-- =============================================================================================================
-- Author:		JHughes
-- Create date: 03/2025
-- Description:	Populate DateDimension table per needs of this solution.
-- =============================================================================================================
CREATE PROCEDURE sp_PopulateDateDimension 
	-- Set date to begin with and how many years to include.
	@StartDate  DATE = '2020-01-01',
	@outYears INT = 30
AS
BEGIN
	DECLARE @CutoffDate date = DATEADD(DAY, -1, DATEADD(YEAR, @outYears, @StartDate));
	DECLARE @CurrentDate date = GETDATE();

	-- We're re-populating, let's start fresh
	TRUNCATE TABLE addin_DateDimension

	;WITH seq(n) AS 
	(
	  SELECT 0 UNION ALL SELECT n + 1 FROM seq
	  WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
	),
	d(d) AS 
	(
	  SELECT DATEADD(DAY, n, @StartDate) FROM seq
	),
	src AS
	(
	  SELECT
		TheDate        = CONVERT(date, d),
		TheDay         = DATEPART(DAY,       d),
		TheDayName     = DATENAME(WEEKDAY,   d),
		TheWeek        = DATEPART(WEEK,      d),
		-- Set time to 00:00:00 for the first of the week
		TheFirstOfWeek = DATEADD(millisecond, 0, DATEADD(DAY, -(DATEPART(WEEKDAY, d) - 1), DATEADD(DAY, DATEDIFF(DAY, 0, d), 0))),
		-- Set time to 23:59:59.997 for the last of the week
		TheLastOfWeek  = DATEADD(MILLISECOND, -3, DATEADD(DAY, 7 - (DATEPART(WEEKDAY, d)), DATEADD(DAY, DATEDIFF(DAY, 0, d) + 1, 0))),
		TheDayOfWeek   = DATEPART(WEEKDAY,   d),
		TheWeekOfMonth = (SELECT CASE 
								WHEN DATEPART(DAY,       d) < 8 THEN 1
								WHEN DATEPART(DAY,       d) < 15 THEN 2
								WHEN DATEPART(DAY,       d) < 22 THEN 3
								WHEN DATEPART(DAY,       d) < 28 THEN 4
								ELSE 5
								END AS WeekOfMonth),
		TheMonth        = DATEPART(MONTH,     d),
		TheMonthName    = DATENAME(MONTH,     d),
		TheQuarter      = DATEPART(Quarter,   d),
		TheYear         = DATEPART(YEAR,      d),
		TheFirstOfMonth = CAST(CAST(DATEFROMPARTS(YEAR(d), MONTH(d), 1) as DATE) as NVARCHAR(30)) + ' 00:00:00.000',
		TheLastOfMonth  = CAST(CAST(EOMONTH(d) as DATE) as NVARCHAR(30)) + ' 23:59:59.997',
		TheLastOfYear   = CAST(CAST(DATEFROMPARTS(YEAR(d), 12, 31) as DATE) as NVARCHAR(30)) + ' 23:59:59.997',
		TheDayOfYear    = DATEPART(DAYOFYEAR, d),
		FastAccess      = CAST(CASE 
							WHEN DATEDIFF(DAY, d, @CurrentDate) <= 90 
								 AND DATEDIFF(DAY, d, @CurrentDate) > 0 THEN 1
							ELSE 0
						  END AS BIT),
		ArcAccess       = CAST(CASE 
							WHEN DATEDIFF(DAY, d, @CurrentDate) > 90 
								 AND DATEDIFF(MONTH, d, @CurrentDate) <= 16 THEN 1
							ELSE 0
						  END AS BIT)
	  FROM d
	)

	-- Insert data into the new table
	INSERT INTO addin_DateDimension
			(TheDate,TheDay,TheDayName,TheWeek
			,TheFirstOfWeek,TheLastOfWeek,TheDayOfWeek
			,TheWeekOfMonth,TheMonth,TheMonthName,TheQuarter
			,TheYear,TheFirstOfMonth,TheLastOfMonth
			,TheLastOfYear,TheDayOfYear,FastAccess,ArcAccess)

			SELECT * FROM src
			ORDER BY TheDate
			OPTION (MAXRECURSION 0);
END
GO

EXECUTE sp_PopulateDateDimension
GO
