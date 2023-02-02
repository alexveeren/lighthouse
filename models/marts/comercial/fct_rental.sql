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

    , rent_payment as (
        select *
        from {{ ref('int_rent__payment') }}
    )

    , join_tabelas as (
        select
            rent_payment.rental_id
            , rent_payment.staff_id
            , rent_payment.inventory_id
            , rent_payment.customer_id
            , rent_payment.payment_id
            , rent_payment.amount
            , rent_payment.payment_date
            , rent_payment.rental_date
            , rent_payment.return_date
            , rent_payment.last_update
        from rent_payment
    )

    , transformed as (
        select
            {{ dbt_utils.surrogate_key(['rental_id', 'fk_category']) }} as sk_vendas
            , *
        from join_tabelas
    )

select *
from transformed