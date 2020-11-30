SELECT * FROM (
	SELECT
		DISTINCT 
		OrderDate, 
		UnitPrice,
		ROW_NUMBER() OVER ( 
				PARTITION BY OrderDate
				ORDER BY UnitPrice DESC
		) PriceOrder,
		ProductKey
		FROM [dbo].[FactInternetSales]
)t
WHERE PriceOrder <= 3
Order BY OrderDate

