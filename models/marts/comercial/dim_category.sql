with 
    film_category as (
            select *
            from {{ ref('stg_erp__film_category') }}
    )

    , category as (
            select *
            from {{ ref('stg_erp__category') }}
    )

    , join_categorias as (
            select
            category.category_id
            , category.type
            , film_category.film_id
            from category
            left join film_category on category.category_id = film_category.category_id
    )

    , transform as (
        select
            row_number() over (order by category_id) as sk_category
            , *
            from join_categorias
    )

select *
from transform
