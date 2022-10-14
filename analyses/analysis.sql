-- challenge 1: describe orders

select * from {{ ref('describe_py') }} limit 10;

select * from {{ ref('describe_sql') }} limit 10;

-- challenge 2: pivot product subtotals onto orders

select * from {{ ref('pivot_py') }} limit 10;

select * from {{ ref('pivot_sql') }} limit 10;

-- challenge 3: unfuzz customer names int_orders

select * from {{ ref('unfuzz_py') }} limit 10;

select * from {{ ref('unfuzz_sql') }} limit 10;

-- use KMeans scikit-learn in Python to cluster orders

select * from {{ ref('cluster_py') }} limit 10;

-- use Prophet in Python to trian forecasting models for revenue

select * from {{ ref('forecast_train_py') }} limit 10;

-- use Prophet in Python to predict revenue

select * from {{ ref('forecast_score_py') }} limit 10;

-- other models

select * from {{ ref('int_orders') }} limit 10;

select * from {{ ref('orders' ) }} limit 10;

select * from {{ ref('customers') }} limit 10;

select * from {{ ref('revenue_weekly_by_location') }} limit 10;
