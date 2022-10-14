-- challenge 1: describe orders

{{ preview(ref('describe_py')) }}

{{ preview(ref('describe_sql')) }}

-- challenge 2: pivot product subtotals onto orders

{{ preview(ref('pivot_py')) }}

{{ preview(ref('pivot_sql')) }}

-- challenge 3: unfuzz customer names int_orders

{{ preview(ref('unfuzz_py')) }}

{{ preview(ref('unfuzz_sql')) }}

-- use KMeans scikit-learn in Python to cluster orders

{{ preview(ref('cluster_py')) }}

-- use Prophet in Python to trian forecasting models for revenue

{{ preview(ref('forecast_train_py')) }}

-- use Prophet in Python to predict revenue

{{ preview(ref('forecast_score_py')) }}

-- other models

{{ preview(ref('int_orders')) }}

{{ preview(ref('orders' )) }}

{{ preview(ref('customers')) }}

{{ preview(ref('revenue_weekly_by_location')) }}
