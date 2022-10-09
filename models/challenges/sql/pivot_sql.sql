
with

orders as (

    select * from {{ ref('orders') }}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

{% set ref_products = ref('stg_products') %}
products as (
  
    select * from {{ ref_products }}
  
),

joined as (
  
    select
    
        orders.*,
        product_id,
        product_price
        
    from orders
    left join order_items using (order_id)
    left join products using (product_id)
  
),

pivoted as (

    select
    
        order_id,
        location_id,
        customer_id_match,
        order_total,
        tax_paid,
        ordered_at,

        sum(product_price) as subtotal,

        {% set product_ids = dbt_utils.get_column_values(ref_products, 'product_id') %}
        {% for product_id in product_ids %}
        
          sum(
            case when product_id = '{{ product_id }}'
            then product_price
            else 0 end
          ) as subtotal_{{ product_id | replace('-', '_') }}{{ ',' if not loop.last }}
        
        {% endfor %}
      
    from joined
    {{ dbt_utils.group_by(6) }}
    
)

select * from pivoted
