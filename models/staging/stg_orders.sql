
with 

source as (

    select * from {{ source('ecommerce', 'orders') }}

    --- data runs to 2026, truncate timespan to desired range, current time as default
    where ordered_at <= {{ var('truncate_timespan_to') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        location_id,
        customer_id,

        ---------- properties
        {{ cents_to_dollars('order_total', 2) }} as order_total,
        {{ cents_to_dollars('tax_paid', 2) }} as tax_paid,

        ---------- timestamps
        ordered_at

    from source

)

select * from renamed
