with
    source_category as (
        select
            cast(category_id as int) as category_id
            , cast(name as string) as type
            , cast(last_update as timestamp) as last_update_category
        from {{ source('discorama', 'category') }}
    )

select *
from source_category