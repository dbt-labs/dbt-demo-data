/*
    We compute a view statistics for each numeric column to match
    describe() or summary() methods from Python dataframes.

    Snowpark's .describe() returns min, max, and count for non-numeric
    columns. NULLs are used for stddev and mean. TODO: Adapt this. 
*/

{% set ref_orders = ref('orders') %}

with 

orders as (

    select * from {{ ref_orders }}

),

described as (

    {% set columns = adapter.get_columns_in_relation(ref_orders) %}
    {% set numeric_cols = [] %}
    {% for col in columns %}
        {% if col.data_type in ('NUMBER', 'FLOAT') %}
            {% do numeric_cols.append(col) %}
        {% endif %}
    {% endfor %}
    
    {% set stats = {
        'stddev': 'stddev(...)',
        'min': 'min(...)',
        'mean': 'avg(...)',
        'count': 'count(...)',
        'max': 'max(...)',
    } %}
    
    {% for stat_name, stat_calc in stats.items() %}
    
    select
    '{{ stat_name }}' as metric,
    {% for col in numeric_cols %}
        {{ stat_calc | replace('...', col.name) }} as {{ col.name }}{{ ',' if not loop.last }}
    {% endfor %}
    
    from {{ ref_orders }}
      
    {{ 'union all' if not loop.last }}
    
    {% endfor %}
  
)

select * from described

{#  
    -- adapated to match Snowpark describe API
    -- keeping pandas in this comment for reference

    {% set stats = {
        'count': 'count(...)',
        'mean': 'avg(...)',
        'std': 'stddev(...)',
        'min': 'min(...)',
        '25%': 'percentile_approx(..., 0.25)',
        '50%': 'percentile_approx(..., 0.50)',
        '75%': 'percentile_approx(..., 0.75)',
        'max': 'max(...)',
    } %}
 
#}