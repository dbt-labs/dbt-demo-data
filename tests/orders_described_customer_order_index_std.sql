select
    metric,
    customer_order_index
from {{ ref('orders_described') }}
where metric = "std"
and customer_order_index < 100
