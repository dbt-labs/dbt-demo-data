
with 

source as (

    select * from {{ source('ecommerce', 'customers') }}

),

renamed as (

    select

        ----------  ids
        id as customer_id_new_column,

        ---------- properties
        name as customer_name

    from source

)

select * from renamed
