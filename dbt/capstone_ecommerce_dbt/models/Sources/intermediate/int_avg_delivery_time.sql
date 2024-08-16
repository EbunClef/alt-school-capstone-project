with delivery_time as (
    select 
        order_id,
        order_status,
        order_purchase_timestamp,
        order_delivered_customer_date,
        TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, minute) as delivery_time_diff
    from 
        {{ ref('stg_orders') }}
    where 
        order_status = 'delivered'
        and order_delivered_customer_date is not null
        and order_purchase_timestamp is not null
)
select 
    order_id,
    avg(delivery_time_diff) as avg_delivery_time
from 
    delivery_time
group by 
    order_id