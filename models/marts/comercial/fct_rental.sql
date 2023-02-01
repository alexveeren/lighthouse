with
    rental as (
        select *
        from {{ ref('stg_erp__rental') }}
    )
    , payment as (
        select *
        from {{ ref('stg_erp__payment') }}
    )

    , uniao_tabelas as (
        select
            rental.rental_id
            , rental.staff_id
            , rental.inventory_id
            , rental.customer_id
            , rental.rental_date
            , rental.return_date
            , rental.last_update
            , payment.payment_id
            , payment.customer_id
            , payment.staff_id
            , payment.rental_id
            , payment.amount
            , payment.payment_date
        from rental
        left join payment on rental.rental_id = payment.rental_id
    )
select *
from uniao_tabelas