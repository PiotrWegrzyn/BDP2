INSERT INTO [dbo].[AUDIT_TABLE] VALUES ( 100, (SELECT COUNT(1) FROM [dbo].[CSV_Customers]),  GETDATE(), NULL );
