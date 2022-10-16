
with

orders as (

    select * from {{ ref('stg_orders') }}

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

product_cost_summary as (

    select

        product_id,
        sum(supplies.supply_cost) as product_cost

    from supplies

    group by 1

),

location_product_summary as (

    select

        location_id,
        any_value(location_name) as location_name,

        count(*) as count_items,
        sum(products.is_food_item) as count_food_items,
        sum(products.is_drink_item) as count_drink_items,
        count(distinct customer_id) as unique_customers,

        sum(product_price) as revenue,
        sum(product_cost) as cost_of_goods_sold,
        revenue - cost_of_goods_sold as gross_profit,
        gross_profit / revenue as profit_margin,
        gross_profit / unique_customers as gross_profit_per_customer

    from order_items
    join products using (product_id)
    join orders using (order_id)
    join locations using (location_id)
    join product_cost_summary using (product_id)

    group by 1
    order by 1

),

final as (

    select 
        
        *

    from location_product_summary

)

select * from final
