/*
    Materialize the revenue metric into a table
    on the desired grain by the desired dimension
    for forecasting by Prophet.
*/

select
    *
from {{ metrics.calculate(
    metric('revenue'),
    grain='week',
    dimensions=['location_name']
) }}
