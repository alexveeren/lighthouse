with 
    film_category as (
            select *
            from {{ ref('stg_erp__film_category') }}
    )

    , film as (
            select *
            from {{ ref('stg_erp__film') }}
    )

    , category as (
            select *
            from {{ ref('stg_erp__category') }}
    )

    , inventory as (
            select *
            from {{ ref('stg_erp__inventory') }}
    )

    , join_film as (
            select
            film_category.film_id
            , film.language_id
            , category.category_id
            , film.title
            , film.release_year
            , film.rental_duration_days
            , film.rental_rate
            , film.duration_in_min
            , film.replacement_cost
            , category.type
            , film_category.last_update_film_category
            , inventory.inventory_id
            , inventory.store_id
            from film_category
            left join film on film_category.film_id = film.film_id
            left join category on film_category.category_id = category.category_id
            left join inventory on film_category.film_id = inventory.film_id
    )

    , transform as (
        select
            row_number() over (order by film_id) as sk_film
            , *
            from join_film
    )

select *
from transform
            