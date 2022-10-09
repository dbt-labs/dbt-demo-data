
with

unfuzzed as (

    select 
        location_id,
        order_id,
        order_total,
        tax_paid,
        -- issue with timestamps between pandas and Snowpark
        to_timestamp_ntz(ordered_at / 1000000) as ordered_at,
        customer_name,
        customer_id_match,
        customer_name_match,
        match_likelihood

    from {{ ref('unfuzz_sql') }}

),

-- whoops
orders as (
    
    select
        *
    from (
        select
            *,
            row_number() over (partition by order_id order by match_likelihood desc) as rn
        from unfuzzed
    )
    where rn = 1
),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

locations as (

    select * from {{ ref('stg_locations') }}

),

supplies as (

    select * from {{ ref('stg_supplies') }}

),

order_items_summary as (

    select

        order_id,

        sum(products.is_food_item) as count_food_items,
        sum(products.is_drink_item) as count_drink_items,
        count(*) as count_items,

        sum(case when products.is_food_item = 1 then product_price else 0 end) as subtotal_drink_items,
        sum(case when products.is_drink_item = 1 then product_price else 0 end) as subtotal_food_items,
        sum(product_price) as subtotal

    from order_items
    join products using (product_id)

    group by 1

),

order_supplies_summary as (

    select

        order_id,

        sum(supplies.supply_cost) as order_cost

    from order_items
    join supplies using (product_id)

    group by 1

),

joined as (

    select

        orders.*,

        order_items_summary.count_food_items,
        order_items_summary.count_drink_items,
        order_items_summary.count_items,

        order_items_summary.subtotal_drink_items,
        order_items_summary.subtotal_food_items,
        order_items_summary.subtotal,

        order_supplies_summary.order_cost,

        -- rank this order for the customer
        row_number() over (
            partition by orders.customer_id_match
            order by orders.ordered_at
        ) as customer_order_index,

        locations.location_name

    from orders
    join order_items_summary using (order_id)
    join order_supplies_summary using (order_id)
    join locations using (location_id)

),

final as (

    select 
        
        *,
        customer_order_index = 1 as is_first_order,
        count_food_items > 0 as is_food_order,
        count_drink_items > 0 as is_drink_order

    from joined

)

select * from final
