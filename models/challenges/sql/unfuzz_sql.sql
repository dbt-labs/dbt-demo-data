/*

    TODO: needs updating

    Map each customer with a likely match, to the lowest ID among matches.
    Returns four columns:
        - original_id
        - original_name
        - map_to_id
        - map_to_name
    
    Using this approach, there's still a risk of missing a multi-step match
    E.g. Let's say we have three names, in ascending order by ID: "Stacy Mater", "Stacey Mater", and "Stacey Mater"
    We'd map Stacey to Stacy, but we'd still be mapping Stacee to Stacey
    We can add an additional mapping step (self-join / recursive CTE) to resolve.
    For now, we could add a test for this: check if one row's map_to_id == original_id in another row:
      {#
          with fuzzy_matches as (
              select * from {{ ref('unfuzzed_sql') }}
          )
          
          select * from fuzzy_matches a
          cross join fuzzy_matches b
          where a.map_to_id = b.original_id
      #}
*/

with customer_names as (

        -- full demo dataset
        select
            customer_id as id,
            customer_name as name
        
        from {{ ref('int_customers') }}
        
),

order_names as (

    select

        order_id as id,
        customer_name as name
    
    from {{ ref('int_orders') }}
    -- limit this for quicker development
    --limit 100 offset 12000

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

orders as (

    select
        *
    from {{ ref('int_orders') }}
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

)

select * from final

/*
dedupe as (

    select distinct
    
        -- for each id/name combo where we've detected a positive match
        id_b as original_id,
        name_b as original_name,
        -- let's map to the lowest id of all possible matches
        min(id_a) over (partition by id_b) as map_to_id,
        first_value(name_a) over (partition by id_b order by id_a) as map_to_name
    
    from matches

)

select * from dedupe
--order by original_name
*/
