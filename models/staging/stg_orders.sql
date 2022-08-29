
with source as (

    select * from {{ source('ecommerce', 'orders') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        location_id,
        customer_id,

        ---------- properties
        order_total,
        tax_paid,

        ---------- timestamps
        ordered_at

    from source

)

select * from renamed
