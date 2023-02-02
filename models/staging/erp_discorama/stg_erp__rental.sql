with
    source_rental as (
        select
            cast(rental_id as int) as rental_id
            , cast(staff_id as int) as staff_id
            , cast(inventory_id as int) as inventory_id
            , cast(customer_id as int) as customer_id
            , cast(rental_date as DATETIME) as rental_date
            , cast(return_date as DATETIME) as return_date
            , cast(last_update as DATETIME) as last_update
        from {{ source('discorama', 'rental') }}
    )

select *
from source_rental