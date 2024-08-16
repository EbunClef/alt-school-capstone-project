with order_items as (
    select 
        order_id,
        product_id,
        price,
        freight_value
    from 
        {{ source('sources', 'order_items') }}
)
select 
    *
from
    order_items