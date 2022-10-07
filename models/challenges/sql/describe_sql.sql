
{% set ref_orders = ref('orders') %}

with

orders as (

    select * from {{ ref_orders }}

),

described as (

    {% set columns = adapter.get_columns_in_relation(ref_orders) %}
    {% set numeric_cols = [] %}
    {% for col in columns %}
        {% if col.data_type in ('float', 'bigint', 'double', 'int') %}
            {% do numeric_cols.append(col) %}
        {% endif %}
    {% endfor %}
    
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
