with
    source_staff as (
        select
            cast(staff_id as int) as staff_id
            , cast(store_id as int) as store_id
            , cast(address_id as int) as address_id
            , cast(first_name as string) as first_name
            , cast(last_name as string) as last_name
            --, cast(email as ) as
            --, cast(active as ) as
            --, cast(username as ) as
            --, cast(password as ) as
            --, cast(last_update as ) as
            --, cast(picture as ) as
        from {{ source('discorama', 'staff') }}
    )

select *
from source_staff