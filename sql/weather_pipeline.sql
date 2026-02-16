create schema if not exists analytics;

create or replace procedure analytics.run_weather_pipeline()
language plpgsql
as $$
begin
    
drop table if exists weather_data_forecast_vs_actual;

-- forecast tmp
drop table if exists weather_data_forecast_tmp;
create temp table weather_data_forecast_tmp as
select
    date as date_f,
    (fetched_at at time zone 'europe/berlin')::date as fetch_date_f,
    date_trunc('hour', (fetched_at at time zone 'europe/berlin')::time) as fetch_time_f,
    date_trunc('hour', (date at time zone 'europe/berlin')::time) as date_time_f,
    date_trunc('hour', fetched_at at time zone 'europe/berlin') as fetched_at_f,
    temperature as temperature_f
from weather_data_forecast
where 
	(date_trunc('hour', (date at time zone 'europe/berlin')::time) =
     date_trunc('hour', (fetched_at at time zone 'europe/berlin')::time)) and
	 (date at time zone 'europe/berlin')::date <> (fetched_at at time zone 'europe/berlin')::date	
;

-- actual tmp
drop table if exists weather_data_actual_tmp;
create temp table weather_data_actual_tmp as
select
    date_trunc('hour', fetched_at at time zone 'europe/berlin') as date_a,
    temperature as temperature_a
from weather_data_actual;

-- forecast vs. actual tmp
drop table if exists weather_data_forecast_vs_actual_tmp;
create temp table weather_data_forecast_vs_actual_tmp as
select
	wdfa.*,
	wdt.tolerance_in_degree 
from (
	select 
        wdft.date_f,
        wdft.fetched_at_f,
		(wdft.date_f::date - wdft.fetched_at_f::date) as forecast_horizon_days,
        wdft.temperature_f,
        wdat.temperature_a
    from weather_data_forecast_tmp as wdft
    left join weather_data_actual_tmp as wdat
           on wdft.date_f = wdat.date_a
	) as wdfa
	join weather_data_tolerance as wdt 
		on wdfa.forecast_horizon_days = wdt.horizon_days
    order by fetched_at_f, date_f
;

-- stats per horizon day
create table weather_data_forecast_vs_actual as
with base as (
    select
        *,
        -- tolerance bounds
        temperature_f + tolerance_in_degree as upper_bound_f,
        temperature_f - tolerance_in_degree as lower_bound_f,

        -- error
        temperature_f - temperature_a as error_val,
        
        -- absolute error
        abs(temperature_f - temperature_a) as abs_error_val,
        (temperature_f - temperature_a)^2 as sq_error_val,

        -- hit flag
        (
            temperature_a between
                temperature_f - tolerance_in_degree
                and temperature_f + tolerance_in_degree
        )::int as hit_flag
    from weather_data_forecast_vs_actual_tmp
),
stats_per_horizon as (
    select
        *,

        -- horizon-wide statistics
        
        -- mean error (me | bias)
        avg(error_val) over (
            partition by forecast_horizon_days
        ) as horizon_bias,

        -- mean absolute error (mae)
        avg(abs_error_val) over (
            partition by forecast_horizon_days
        ) as horizon_mae,

        -- root mean squared error (rmse)
        sqrt(
            avg(sq_error_val) over (
                partition by forecast_horizon_days
            )
        ) as horizon_rmse,
	
        -- weighted mean absolute percentage error (wmape) 
        sum(abs_error_val) over (
            partition by forecast_horizon_days
        )
        / nullif(
            sum(abs(temperature_f)) over (
                partition by forecast_horizon_days
            ),
            0
        ) * 100 as horizon_wmape,

        -- symmetric mean absolute percentage error (smape)
        avg(
            2 * abs_error_val
            / nullif(abs(temperature_f) + abs(temperature_a), 0)
        ) over (
            partition by forecast_horizon_days
        ) * 100 as horizon_smape,

        (
            sum(hit_flag) over (
                partition by forecast_horizon_days
            )::numeric
            / count(*) over (
                partition by forecast_horizon_days
            )
        ) * 100 as horizon_hit_rate,
		
		-- mae/rmse
		(
			avg(abs_error_val) over (
            	partition by forecast_horizon_days
        	)
		)
		/ 
		nullif(
			sqrt(
	            avg(sq_error_val) over (
	                partition by forecast_horizon_days
	        	)
	        ), 
			0
		) as horizon_q_mae_rmse

    from base
)
select
    sh.*,
    
    -- statistics over time
    
    -- mean error (bias)
    avg(error_val) over (
        partition by forecast_horizon_days
        order by date_f
        rows between unbounded preceding and current row
    ) as cum_bias,

    -- mean absolute error (mae)
    avg(abs_error_val) over (
        partition by forecast_horizon_days
        order by date_f
        rows between unbounded preceding and current row
    ) as cum_mae,

    -- root mean squared error (rmse) 
    sqrt(
        avg(sq_error_val) over (
            partition by forecast_horizon_days
            order by date_f
            rows between unbounded preceding and current row
        )
    ) as cum_rmse,

    -- weighted mean absolute percentage error (wmape) 
    sum(abs_error_val) over (
        partition by forecast_horizon_days
        order by date_f
        rows between unbounded preceding and current row
    )
    / nullif(
        sum(abs(temperature_f)) over (
            partition by forecast_horizon_days
            order by date_f
            rows between unbounded preceding and current row
        ),
        0
    ) * 100 as cum_wmape,

    -- symmetric mean absolute percentage error (smape)
    avg(
        2 * abs_error_val
        / nullif(abs(temperature_f) + abs(temperature_a), 0)
    ) over (
        partition by forecast_horizon_days
        order by fetched_at_f
        rows between unbounded preceding and current row
    ) * 100 as cum_smape,

    -- hit rate
    (
        sum(hit_flag) over (
            partition by forecast_horizon_days
            order by date_f
            rows between unbounded preceding and current row
        )::numeric
        / count(*) over (
            partition by forecast_horizon_days
            order by date_f
            rows between unbounded preceding and current row
        )
    ) * 100 as cum_hit_rate

from stats_per_horizon sh;

end;
$$;



