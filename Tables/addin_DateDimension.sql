USE [GADATA_RPT]
GO

CREATE TABLE addin_DateDimension (
    TheDate         date PRIMARY KEY,
    TheDay          int,
    TheDayName      varchar(20),
    TheWeek         int,
	TheFirstOfWeek	datetime,
	TheLastOfWeek	datetime,
    TheDayOfWeek    int,
    TheWeekOfMonth  int,
    TheMonth        int,
    TheMonthName    varchar(20),
    TheQuarter      int,
    TheYear         int,
    TheFirstOfMonth datetime,
	TheLastOfMonth	datetime,
    TheLastOfYear   datetime,
    TheDayOfYear    int,
    FastAccess      bit,
    ArcAccess       bit
)
GO