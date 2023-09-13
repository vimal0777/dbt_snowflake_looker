WITH retailers AS (
    SELECT RETAILER_CODE,
           RETAILER_NAME AS COMPANY_NAME,
           TYPE AS COMPANY_TYPE,
           COUNTRY
    FROM {{ ref('stg_retailers') }}
),

daily_sales AS (
    SELECT RETAILER_CODE,
           PRODUCT_NUMBER,
           ORDER_METHOD_CODE,
           Date_ AS ORDERED_DATE,
           QUANTITY AS QUANTITY_SOLD,
           SELLING_PRICE,
           COST_PRICE

    FROM {{ ref('stg_daily_sales') }}
),

retailers_daily_sales as (
    SELECT *
    FROM retailers
    INNER JOIN
    daily_sales
    USING(RETAILER_CODE)
)
SELECT  * FROM retailers_daily_sales