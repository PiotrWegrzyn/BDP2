/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [EmailAddress]
      ,[LastName]
      ,[FirstName]
      ,[Address]
      ,[City]
      ,[PhoneNumber]
      ,[CREATE_TIMESTAMP]
      ,[UPDATE_TIMESTAMP]
  FROM [AdventureWorksDW2019].[dbo].[CSV_Customers]
  order by UPDATE_TIMESTAMP desc, CREATE_TIMESTAMP desc