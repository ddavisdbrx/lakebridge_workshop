SELECT
    customer_id,
    current_timestamp() AS query_time
FROM lakebridge.DimCustomer
WHERE customer_id <= 5;
