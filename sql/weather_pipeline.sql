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
    
    DROP TABLE IF EXISTS weather_data_forecast_vs_current;

    -- Forecast_tmp
	DROP TABLE IF EXISTS weather_forecast_tmp;
    CREATE TEMP TABLE weather_forecast_tmp AS
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
	DROP TABLE IF EXISTS weather_current_tmp;
    CREATE TEMP TABLE weather_current_tmp AS
    SELECT
        date_trunc('hour', fetched_at AT TIME ZONE 'Europe/Berlin') AS date_c,
        temperature AS temperature_c,
        humidity AS humidity_c
    FROM weather_data_current;

	-- Final table (Forecast vs. Current)
    CREATE TABLE weather_data_forecast_vs_current AS
    SELECT 
        wdfc.date_f,
        wdfc.fetched_at_f,
		analytics.forecast_horizon_days(wdfc.date_f, wdfc.fetched_at_f) AS forecast_horizon_days,
        wdfc.temperature_f,
        wdcc.temperature_c,
        wdfc.humidity_f,
        wdcc.humidity_c
    FROM weather_forecast_tmp AS wdfc
    LEFT JOIN weather_current_tmp AS wdcc
           ON wdfc.date_f = wdcc.date_c
    ORDER BY fetched_at_f, date_f;

END;
$$;



