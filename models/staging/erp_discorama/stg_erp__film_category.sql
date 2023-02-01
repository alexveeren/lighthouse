with
    source_film_category as (
        select
            cast(film_id as int) as film_id
            , cast(category_id as int) as category_id
            , cast(last_update as timestamp) as last_update_film_category
        from {{ source('discorama', 'film_category') }}
    )

select *
from source_film_category