DROP TABLE IF EXISTS dbo.stg_dimemp;
SELECT [EmployeeKey], [FirstName], [LastName], [Title]
INTO dbo.stg_dimemp
FROM [dbo].[DimEmployee]
WHERE [EmployeeKey] BETWEEN 270 AND 275;

DROP TABLE IF EXISTS dbo.scd_dimemp;


CREATE TABLE dbo.scd_dimemp(
EmployeeKey int,
StartDate datetime, 
EndDate datetime,
FirstName nvarchar(50) not null,
LastName nvarchar(50) not null,
Title nvarchar(50)
);

