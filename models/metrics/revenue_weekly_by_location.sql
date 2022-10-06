select
    *
from {{ metrics.calculate(
    metric('revenue'),
    grain='week',
    dimensions=['location_name']
) }}
