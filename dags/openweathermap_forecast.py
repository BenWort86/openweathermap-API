from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from openweathermap.weather_forecast import run
import pendulum

local_tz = pendulum.timezone("Europe/Berlin")

with DAG(
    dag_id='weather_forecast',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule='0 1,4,7,10,13,16,19,22 * * *',
    catchup=False,
) as dag:

    run_job = PythonOperator(
        task_id='weather_forecast',
        python_callable=run,
    )
