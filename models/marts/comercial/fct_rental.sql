with
    store as (
        select *
        from {{ ref('dim_store') }}
    )

    , category as (
        select *
        from {{ ref('dim_category') }}
    )

    , dates as (
        select *
        from {{ ref('dim_date') }}
    )

    , film as (
        select *
        from {{ ref('dim_film') }}
    )

    , rent_payment as (
        select *
        from {{ ref('int_rent__payment') }}
    )

    , join_tabelas as (
        select
            rent_payment.rental_id
            , film.sk_film as fk_film
            , store.sk_store as fk_store
            , rent_payment.staff_id
            , rent_payment.inventory_id
            , rent_payment.customer_id
            , rent_payment.payment_id
            , rent_payment.amount
            , cast(rent_payment.payment_date as datetime) as payment_date
            , dates.trimestre as payment_trimestre
            , rent_payment.rental_date
            , rent_payment.return_date
            , rent_payment.last_update
        from rent_payment
        left join film on rent_payment.inventory_id = film.inventory_id
        left join store on rent_payment.staff_id = store.staff_id
        left join dates on rent_payment.payment_date = dates.date_day
    )

    , transformed as (
        select
            {{ dbt_utils.surrogate_key(['rental_id', 'staff_id']) }} as sk_vendas
            , *
        from join_tabelas
    )

select *
from transformed