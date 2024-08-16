with products as (
    select 
        cast(product_id as string) as product_id,
        product_category_name
    from 
        {{ source('sources', 'products') }}
)
select
    * 
from
    products