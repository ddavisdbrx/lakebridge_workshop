--NVARCHAR(100) should be converted to STRING upon conversion to Databricks syntax
--dbo.DimProdct should be converted to the correct schema and table name upon conversion to Databricks syntax
SELECT CAST(product_name AS NVARCHAR(100)) AS prod_name
FROM dbo.DimProduct;