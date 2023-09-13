with products as (
    select * from {{ source('mn_stores', 'products') }}
)
select * from products