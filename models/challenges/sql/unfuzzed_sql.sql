/*

    TODO: rework per unfuzz_py.py

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

with all_names as (

        
        -- full demo dataset
        select
            customer_id as id,
            customer_name as name
        
        from {{ ref('stg_customers') }}
        
        /*
        -- tiny test dataset for iterative SQL development
        select 1 as id, 'Cody Peterson' as name  union all
        select 2,       'Cody Pederson'          union all
        select 3,       'Codey Peterson'         union all
        select 4,       'Cody P'                 union all
        select 5,       'CodyPeterson'           union all
        select 6,       'Jeremy Cohen'           union all
        select 7,       'Jerco'                  union all
        select 8,       'JerCo'                  union all
        select 9,       'Jeremey Cohen'          union all
        select 10,      'Jérémy Cohen'
        */
    
        /*
        -- example for limitation discussed above
        select 1 as id, 'Stacy Mater' as name  union all
        select 2,       'Stacey Mater' union all
        select 3,       'Stacee Mater'
        */
    
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
    
    from all_names a
    cross join all_names b
    -- don't compare to self, and only compare in one order (lesser id < greater id) to avoid inverted dupes
    where a.id < b.id

),

matches as (

    select id_a, name_a, id_b, name_b
    from cross_joined
    -- bump this number for fewer positive matches (both true positives & false positives)
    -- i.e. the higher the ratio, the more specific & the less sensitive
    where levenshtein_ratio >= 0.9

),

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
