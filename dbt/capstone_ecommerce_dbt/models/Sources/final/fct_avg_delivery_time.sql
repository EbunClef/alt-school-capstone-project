select
    avg(avg_delivery_time) as overall_avg_delivery_time
from 
    {{ ref('int_avg_delivery_time') }}
where
    avg_delivery_time is not null