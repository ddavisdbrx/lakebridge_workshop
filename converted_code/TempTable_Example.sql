-- Example Synapse SQL Script Using a Temp Table

-- 1. Create a temp table from FactInternetSales

CREATE TEMPORARY TABLE TEMP_TABLE_TempSales AS
SELECT 
    fs.ProductKey,
    fs.OrderDateKey,
    fs.SalesOrderNumber,
    fs.OrderQuantity,
    fs.UnitPrice,
    fs.ExtendedAmount

FROM `lakebridge`.FactInternetSales fs
WHERE fs.OrderQuantity > 1;
-- 2. Enrich temp table with product details

CREATE TEMPORARY TABLE TEMP_TABLE_TempSalesWithProduct AS
SELECT 
    ts.ProductKey,
    p.ProductAlternateKey,
    p.EnglishProductName,
    ts.OrderQuantity,
    ts.UnitPrice,
    ts.ExtendedAmount,
    p.Color,
    p.Size

FROM TEMP_TABLE_TempSales ts
JOIN ddavis_demo.lakebridge.dim_product p
    ON ts.ProductKey = p.ProductKey;
-- 3. Use second temp table in a CTE

WITH SalesSummary AS (
    SELECT 
        ProductKey,
        SUM(OrderQuantity) AS TotalQty,
        SUM(ExtendedAmount) AS TotalSales
    FROM TEMP_TABLE_TempSalesWithProduct
    GROUP BY ProductKey
)

-- 4. Join CTE to DimProduct to produce final report
SELECT 
    ss.ProductKey,
    p.EnglishProductName,
    ss.TotalQty,
    ss.TotalSales
FROM SalesSummary ss
JOIN ddavis_demo.lakebridge.dim_product p
    ON ss.ProductKey = p.ProductKey
ORDER BY ss.TotalSales DESC;
-- 5. Cleanup

DROP TEMPORARY TABLE IF EXISTS TEMP_TABLE_TempSales;
DROP TEMPORARY TABLE IF EXISTS TEMP_TABLE_TempSalesWithProduct;
