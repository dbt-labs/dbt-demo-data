/*
    TODO: document logic
*/

with customer_names as (

        -- full demo dataset
        select
            customer_id as id,
            customer_name as name
        
        from {{ ref('int_customers') }}
        
),

orders as (

    select
        *
    from {{ ref('int_orders') }}
),


order_names as (

    select

        order_id as id,
        customer_name as name
    
    from orders
    -- limit this for quicker development
    --limit 100 offset 100

),

cross_joined as (

    select
    
        a.id as id_a,
        a.name as name_a,
        b.id as id_b,
        b.name as name_b,
    
        -- https://docs.snowflake.com/en/sql-reference/functions/editdistance.html
        editdistance(a.name, b.name) as levenshtein_distance,
        greatest(len(a.name), len(b.name)) as max_length,
        -- https://stackoverflow.com/questions/14260126/how-python-levenshtein-ratio-is-computed
        (1 - levenshtein_distance/max_length) as levenshtein_ratio
    
    from order_names a
    cross join customer_names b

),

matches as (

    select 
        id_a,
        name_a, 
        id_b, 
        name_b,
        levenshtein_ratio as match_likelihood
    from cross_joined
    -- bump this number for fewer positive matches (both true positives & false positives)
    -- i.e. the higher the ratio, the more specific & the less sensitive
    where levenshtein_ratio >= 0.50
    order by id_a, match_likelihood desc

),

final as (

    select
        orders.*,
        matches.id_b as customer_id_match,
        matches.name_b as customer_name_match,
        matches.match_likelihood as match_likelihood
    from orders
    left join matches 
    on orders.order_id = matches.id_a


),

final_filtered as(
    
    select
        *
    from (
        select
            *,
            row_number() over (partition by order_id order by match_likelihood desc) as rn
        from final
    )
    where rn = 1
)

select * from final_filtered
