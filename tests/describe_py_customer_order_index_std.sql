select
    metric,
    customer_order_index
from {{ ref('describe_py') }}
where metric = "std"
and customer_order_index < 100
