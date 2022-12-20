-- test change from VS Code
with 
-- change 2 from VS Code
source as (

    select * from {{ source('ecommerce', 'customers') }}

),

renamed as (

    select

        ----------  ids
        id as customer_id,

        ---------- properties
        name as customer_name

    from source

)

select * from renamed
