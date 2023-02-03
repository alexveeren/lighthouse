with 
    store as (
        select
            store_id
            , manager_staff_id
            , address_id
        from {{ ref('stg_erp__store') }}
    )

    , staff as (
        select *
        from {{ ref('stg_erp__staff') }}
    )

    , joined as (
        select
            store.store_id
            , store.manager_staff_id
            , store.address_id 
            , staff.staff_id
            , staff.first_name
            , staff.last_name
        from store
        left join staff on store.store_id = staff.store_id
    )

    , transform as (
        select
            row_number() over (order by store_id) as sk_store
            , *
        from joined
    )

select *
from transform