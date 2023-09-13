WITH sales_with_year_quarter AS (
    SELECT
        product,
        financialquarter,
        ordered_year,
        country,
        quantity_sold
    FROM
         {{ ref('int_retailers_and_daily_sales') }}
    WHERE
        EXTRACT(YEAR FROM date_) IN (2015, 2016, 2017)
),

-- Final query to calculate sales by year and quarter
fin_qu AS (
    SELECT
    product,
    ordered_year,
    financialquarter,
    country,
    SUM(quantity_sold) AS total_quantity_sold
FROM
    sales_with_year_quarter
GROUP BY 
    product,ordered_year,financialquarter, country    
ORDER BY
    product, ordered_year, financialquarter, country
)

SELECT * FROM fin_qu