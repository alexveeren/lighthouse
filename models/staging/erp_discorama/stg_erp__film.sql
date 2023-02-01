with
    source_film as (
        select
            cast(film_id as int) as film_id
            , cast(language_id as string) as language_id
            , cast(title as string) as title
            --, cast(description as string) as 
            , cast(release_year as int) as release_year
            , cast(rental_duration as int) as rental_duration_days
            , cast(rental_rate as numeric) as rental_rate
            , cast(length as int) as duration_in_min
            , cast(replacement_cost as numeric) as replacement_cost
            --, cast(rating as ) as
            , cast(last_update as timestamp) as last_update
            --, cast(special_features as )
            --, cast(fulltext as )
        from {{ source('discorama', 'film') }}
    )

select *
from source_film