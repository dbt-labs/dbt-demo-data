
with

source as (

    select * from {{ ref('src_locations') }}

    --- data runs to 2026, truncate timespan to desired range, current time as default
    where opened <= {{ var('truncate_timespan_to') }}

),

renamed as (

    select

        ----------  ids
        id as location_id,

        ---------- properties
        name as location_name,
        tax as tax_rate,

        ---------- timestamp
        opened as opened_at

    from source

)

select * from renamed
