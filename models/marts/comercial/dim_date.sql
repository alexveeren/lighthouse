with
    dates as(
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2000-01-01' as date)",
            end_date="cast('2010-01-01' as date)"
           )
        }}
    )

    , final as(
        select cast(date_day as date) as date_day
            , extract(month from date_day) as mes
            , extract(quarter from date_day) as trimestre
            , extract(year from date_day) as ano
            , extract(week from date_day) as semana
        from dates
    )

    

select *
from final