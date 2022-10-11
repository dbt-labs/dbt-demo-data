select *
from {{ ref('describe_py') }}
;

select *
from {{ ref('describe_sql') }}
;

select *
from {{ ref('pivot_py') }}
limit 10
;

select *
from {{ ref('pivot_sql') }}
limit 10
;

select *
from {{ ref('unfuzz_py') }}
limit 10
;

select *
from {{ ref('unfuzz_sql') }}
limit 10
;

select *
from {{ ref('cluster_py') }}
limit 10
;

select *
from {{ ref('forecast_train_py') }}
limit 10
;

select *
from {{ ref('forecast_score_py') }}
limit 10
;
