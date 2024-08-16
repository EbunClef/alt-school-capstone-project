with state_orders as(
    select 
        customer_state,
        order_id
    from 
        {{ ref('stg_orders') }} o
    join
        {{ ref('stg_customers') }} c
    on 
        o.customer_id = c.customer_id
)
select 
    customer_state,
    count(distinct order_id) as total_orders
from
    state_orders
group by 
    customer_state