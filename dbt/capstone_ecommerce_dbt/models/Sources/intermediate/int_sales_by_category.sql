with product_sales as(
    select
        p.product_category_name,
        oi.price
    from 
        {{ ref('stg_order_items') }} oi
    join
        {{ ref('stg_products') }} p
    on
        oi.product_id = p.product_id
)
select 
    product_category_name,
    sum(price) as total_sales
from
    product_sales
group by
    product_category_name