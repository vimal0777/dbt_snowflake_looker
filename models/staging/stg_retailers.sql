with retailers as (
    select * from {{ source('mn_stores', 'retailers') }}
)
select * from retailers