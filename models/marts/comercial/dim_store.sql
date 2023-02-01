with 
    store as (
        select
            store_id
            , manager_staff_id
            , address_id
        from {{ ref('stg_erp__store') }}
    )       

    , transform as (
        select
            row_number() over (order by store_id) as sk_store
            , *
        from store
    )

select *
from transform