CREATE OR REPLACE TABLE lakebridge.SalesSummary AS

SELECT ProductKey,
    SUM(OrderQuantity) AS TotalQty,
    SUM(ExtendedAmount) AS TotalSales,
    current_timestamp() as CurrentTimestamp
FROM lakebridge.FactInternetSales
GROUP BY ProductKey;
