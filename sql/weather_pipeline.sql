CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE FUNCTION analytics.forecast_horizon_days(
    forecast_date timestamptz,
    issue_date timestamptz
) RETURNS smallint
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT (forecast_date::date - issue_date::date);
$$;


CREATE OR REPLACE PROCEDURE analytics.run_weather_pipeline()
LANGUAGE plpgsql
AS $$
BEGIN
    
    DROP TABLE IF EXISTS weather_data_forecast_vs_actual;

    -- Forecast_tmp
	DROP TABLE IF EXISTS weather_data_forecast_tmp;
    CREATE TEMP TABLE weather_data_forecast_tmp AS
    SELECT
        date AS date_f,
        (fetched_at AT TIME ZONE 'Europe/Berlin')::date AS fetch_date_f,
        date_trunc('hour', (fetched_at AT TIME ZONE 'Europe/Berlin')::time) AS fetch_time_f,
        date_trunc('hour', (date AT TIME ZONE 'Europe/Berlin')::time) AS date_time_f,
        date_trunc('hour', fetched_at AT TIME ZONE 'Europe/Berlin') AS fetched_at_f,
        temperature AS temperature_f,
        humidity AS humidity_f
    FROM weather_data_forecast
    WHERE 
		(date_trunc('hour', (date AT TIME ZONE 'Europe/Berlin')::time) =
         date_trunc('hour', (fetched_at AT TIME ZONE 'Europe/Berlin')::time)) and
		 (date AT TIME ZONE 'Europe/Berlin')::date <> (fetched_at AT TIME ZONE 'Europe/Berlin')::date	
	;

    -- Current_tmp
	DROP TABLE IF EXISTS weather_data_actual_tmp;
    CREATE TEMP TABLE weather_data_actual_tmp AS
    SELECT
        date_trunc('hour', fetched_at AT TIME ZONE 'Europe/Berlin') AS date_a,
        temperature AS temperature_a,
        humidity AS humidity_a
    FROM weather_data_actual;

	-- Final table (Forecast vs. Current)
    CREATE TABLE weather_data_forecast_vs_actual AS
    SELECT 
        wdft.date_f,
        wdft.fetched_at_f,
		analytics.forecast_horizon_days(wdft.date_f, wdft.fetched_at_f) AS forecast_horizon_days,
        wdft.temperature_f,
        wdat.temperature_a,
        wdft.humidity_f,
        wdat.humidity_a
    FROM weather_data_forecast_tmp AS wdft
    LEFT JOIN weather_data_actual_tmp AS wdat
           ON wdft.date_f = wdat.date_a
    ORDER BY fetched_at_f, date_f;

END;
$$;



