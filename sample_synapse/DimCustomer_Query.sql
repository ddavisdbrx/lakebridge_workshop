SELECT
    customer_id,
    GETDATE() AS query_time
FROM dbo.DimCustomer
WHERE customer_id <= 5;