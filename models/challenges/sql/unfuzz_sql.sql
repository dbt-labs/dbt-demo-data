/*
    For each order, we will compute Levenshtein ratio (required edits / length of string)
    between that order's customer name, and every known customer name.
    Then, we'll take the highest ratio (= closest match).
*/

with customers as (

    -- actual customer names
    select * from {{ ref('int_customers') }}
        
),

orders as (

    -- orders with erroneous customer names
    select * from {{ ref('int_orders') }}
    
),

cross_joined as (

    select
    
        orders.order_id,
        orders.customer_name,
        customers.customer_id as customer_id_match,
        customers.customer_name as customer_name_match,
    
        -- https://docs.snowflake.com/en/sql-reference/functions/editdistance.html
        editdistance(lower(orders.customer_name), lower(customers.customer_name)) as levenshtein_distance,
        greatest(len(orders.customer_name), len(customers.customer_name)) as max_length,
        -- https://stackoverflow.com/questions/14260126/how-python-levenshtein-ratio-is-computed
        (1 - levenshtein_distance/max_length) as levenshtein_ratio
    
    from orders
    cross join customers
    -- don't even consider comparisons that require changing more than half the characters
    where levenshtein_ratio > 0.50

),

filtered as (

    select
    
        order_id,
        customer_name,
        customer_id_match,
        customer_name_match,
        levenshtein_ratio
    
    from cross_joined
    -- in case of multiple matches, take the best match
    qualify row_number() over (partition by order_id order by levenshtein_ratio desc) = 1

)

select * from filtered
