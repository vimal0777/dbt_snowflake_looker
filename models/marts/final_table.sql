WITH sales_with_year_quarter AS (
    SELECT
        product,
        financialquarter,
        ordered_year,
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
    SUM(quantity_sold)
FROM
    sales_with_year_quarter
GROUP BY 
    product,ordered_year,financialquarter    
ORDER BY
    product, ordered_year, financialquarter
)

SELECT * FROM fin_qu