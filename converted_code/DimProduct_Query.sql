--NVARCHAR(100) should be converted to STRING upon conversion to Databricks syntax
--dbo.DimProdct should be converted to the correct schema and table name upon conversion to Databricks syntax

SELECT CAST(product_name AS STRING) AS prod_name
FROM ddavis_demo.lakebridge.dim_product;
