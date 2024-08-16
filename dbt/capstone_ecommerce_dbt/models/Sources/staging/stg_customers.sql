with customer_orders as (
    select 
        customer_id,
        customer_city,
        customer_state
    from 
        {{ source('sources', 'customers') }}
)
select 
    * 
from 
    customers