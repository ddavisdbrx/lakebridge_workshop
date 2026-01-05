--STRING_AGG function should change upon conversion to Databricks syntax
SELECT
    store_id,
    STRING_AGG(product_id, ',') AS products_sold
FROM dbo.FactTransactionItem
GROUP BY store_id;