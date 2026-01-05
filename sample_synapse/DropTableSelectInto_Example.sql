
DROP TABLE IF EXISTS dbo.SalesSummary;

SELECT 
    ProductKey,
    SUM(OrderQuantity) AS TotalQty,
    SUM(ExtendedAmount) AS TotalSales,
    GETUTCDATE() as CurrentTimestamp
INTO dbo.SalesSummary
FROM dbo.FactInternetSales
GROUP BY ProductKey;