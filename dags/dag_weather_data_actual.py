from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# import the data collection function
from openweathermap.weather_current import run
import pendulum  # for timezone support

# Set local timezone
local_tz = pendulum.timezone("Europe/Berlin")

# Define DAG
with DAG(
    dag_id='weather_actual',  # unique DAG id
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),  # start date
    schedule='0 1,4,7,10,13,16,19,22 * * *',  # run every 3 hours at specific hours
    catchup=False,  # do not backfill missed runs
) as dag:

    # Task: run the weather collection script
    run_job = PythonOperator(
        task_id='weather_actual',  # task id
        python_callable=run,  # function to execute
    )
