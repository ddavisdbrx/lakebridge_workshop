--STRING_AGG function should change upon conversion to Databricks syntax

SELECT
    store_id,
    ARRAY_JOIN(COLLECT_LIST(product_id), ',') AS products_sold
FROM lakebridge.FactTransactionItem
GROUP BY store_id;
