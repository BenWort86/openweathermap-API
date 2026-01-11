
import requests
from datetime import datetime, timezone
import pandas as pd
import sqlalchemy
from . import config
import logging
from pathlib import Path


WEATHER_DATA_URL = (
    f'https://api.openweathermap.org/data/2.5/weather?lat=53.551086&'
    f'lon=9.993682&appid={config.Config_Api.WEATHER_API_KEY}&units=metric'
)

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=LOG_DIR / "weather_current.log",
    filemode='a',
    force=True
)


def get_weather_data():
    try:
        response = requests.get(WEATHER_DATA_URL)

        if 200 <= response.status_code < 300:
            logging.info('Success! Status code: %s', response.status_code)
            return response

        elif 300 <= response.status_code < 400:
            logging.info('Redirection! Status code: %s', response.status_code)
            return response

        elif 400 <= response.status_code < 500:
            logging.warning('Client error! Status code: %s',
                            response.status_code)
            return response

        elif 500 <= response.status_code < 600:
            logging.error('Server error! Status code: %s',
                          response.status_code)
            return response

    except requests.exceptions.RequestException as e:
        logging.info('Request failed: %s', e)
        return None


def data_transform():

    weather_data = []
    data = get_weather_data()

    if data is None:
        print('No Data!')
        return pd.DataFrame()

    data_json = data.json()

    main = data_json['main']

    date_time = datetime.fromtimestamp(data_json['dt'], timezone.utc)

    weather_data = [{
        'date': date_time,
        'humidity': main.get('humidity'),
        'temperature': main.get('temp'),
        'feels_like': main.get('feels_like'),
        'temperature_max': main.get('temp_max'),
        'temperature_min': main.get('temp_min'),
        'pressure': main.get('pressure'),
        'sea_level': main.get('sea_level'),
        'ground_level': main.get('grnd_level'),
    }]

    weather_data_df = pd.DataFrame(weather_data)
    return weather_data_df


def connect_to_db():
    db_url = config.Config_Database.database_url()
    return sqlalchemy.create_engine(db_url)


def write_to_db(fetched_at):
    table_name = 'weather_data_current'
    weather_data_df = data_transform()

    weather_data_df['fetched_at'] = fetched_at

    engine = connect_to_db()
    with engine.begin() as conn:
        weather_data_df.to_sql(
            table_name, conn, if_exists='append', index=False)
    engine.dispose()


def run():
    fetched_at = datetime.now(timezone.utc)
    write_to_db(fetched_at)


if __name__ == '__main__':
    run()
