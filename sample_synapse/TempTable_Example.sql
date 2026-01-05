-- Example Synapse SQL Script Using a Temp Table

-- 1. Create a temp table from FactInternetSales
SELECT 
    fs.ProductKey,
    fs.OrderDateKey,
    fs.SalesOrderNumber,
    fs.OrderQuantity,
    fs.UnitPrice,
    fs.ExtendedAmount
INTO #TempSales
FROM [EDW].FactInternetSales fs
WHERE fs.OrderQuantity > 1;


-- 2. Enrich temp table with product details
SELECT 
    ts.ProductKey,
    p.ProductAlternateKey,
    p.EnglishProductName,
    ts.OrderQuantity,
    ts.UnitPrice,
    ts.ExtendedAmount,
    p.Color,
    p.Size
INTO #TempSalesWithProduct
FROM #TempSales ts
JOIN dbo.DimProduct p
    ON ts.ProductKey = p.ProductKey;


-- 3. Use second temp table in a CTE
WITH SalesSummary AS (
    SELECT 
        ProductKey,
        SUM(OrderQuantity) AS TotalQty,
        SUM(ExtendedAmount) AS TotalSales
    FROM #TempSalesWithProduct
    GROUP BY ProductKey
)

-- 4. Join CTE to DimProduct to produce final report
SELECT 
    ss.ProductKey,
    p.EnglishProductName,
    ss.TotalQty,
    ss.TotalSales
FROM SalesSummary ss
JOIN dbo.DimProduct p
    ON ss.ProductKey = p.ProductKey
ORDER BY ss.TotalSales DESC;


-- 5. Cleanup
DROP TABLE IF EXISTS #TempSales;
DROP TABLE IF EXISTS #TempSalesWithProduct;
