
with source as (

    select * from {{ source('ecommerce', 'locations') }}

),

renamed as (

    select

        ----------  ids
        id as location_id,

        ---------- properties
        name as location_name,
        tax_rate,

        ---------- timestamp
        opened_at

    from source

)

select * from renamed
