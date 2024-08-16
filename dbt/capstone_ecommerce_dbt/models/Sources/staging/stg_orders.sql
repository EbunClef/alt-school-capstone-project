with raw_orders as (
    select 
        cast(order_id as string) as order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_delivered_customer_date
    from 
        {{ source('sources', 'orders') }}
)
select
    *
from 
    raw_orders