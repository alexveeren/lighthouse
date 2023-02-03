with 
    rent as (
        select
            rental_id
            , staff_id
            , inventory_id
            , customer_id
            , rental_date
            , return_date
            , last_update
        from {{ ref('stg_erp__rental') }}
    )

    , payment as (
        select
            payment_id
            , customer_id
            , staff_id
            , rental_id
            , amount
            , payment_date
        from {{ ref('stg_erp__payment') }}
    )

    , joined as (
        select
            rent.rental_id
            , rent.staff_id
            , rent.inventory_id
            , rent.customer_id
            , payment.payment_id
            , payment.amount
            , payment.payment_date
            , rent.rental_date
            , rent.return_date
            , rent.last_update
        from rent
        left join payment on rent.rental_id = payment.rental_id
    )

select *
from joined