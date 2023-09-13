with
    retailers as (
        select
            retailer_code, retailer_name as company_name, type as company_type, country
        from {{ ref("stg_retailers") }}
    ),

    daily_sales as (
        select
            retailer_code,
            product_number,
            order_method_code,
            date_,
            to_char(date_::date, 'YYYY-Mon-DD') as ordered_date,
            year(date_) as ordered_year,
            concat('Q', '', quarter(date_)) as financialquarter,
            quantity as quantity_sold,
            selling_price,
            cost_price,
            round ((selling_price-cost_price)*quantity_sold, 0) AS profit
        from {{ ref("stg_daily_sales") }}
    ),

    products AS (
        SELECT * FROM {{ ref('stg_products') }}
    ),

    order_type AS (
        SELECT * FROM {{ ref('stg_order_methods') }}
    ),



    retailers_daily_sales as (
        select * from retailers inner join daily_sales using (retailer_code)
    ),

    ret_das_pr AS (
        select * 
        from retailers_daily_sales
        INNER JOIN
        products
        USING(PRODUCT_NUMBER)
    ),

    ret_das_pr_od AS (
        SELECT * 
        FROM ret_das_pr
        INNER JOIN
        order_type
        USING(ORDER_METHOD_CODE)
    )

select * from ret_das_pr_od