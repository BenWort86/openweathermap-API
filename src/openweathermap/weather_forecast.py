import requests  # HTTP requests
from datetime import datetime, timezone  # timestamps with timezone
import pandas as pd  # dataframes
import sqlalchemy  # DB connection
from . import config  # API keys and DB config
import logging  # logging
from pathlib import Path  # file paths


# OpenWeatherMap forecast API for Hamburg
WEATHER_DATA_URL = (
    f'https://api.openweathermap.org/data/2.5/forecast?lat=53.551086&'
    f'lon=9.993682&appid={config.Config_Api.WEATHER_API_KEY}&units=metric'
)

# Base and log directories
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=LOG_DIR / "weather_forecast.log",
    filemode='a',
    force=True
)


def get_weather_data():
    """Fetch weather data from API and handle response codes."""
    try:
        response = requests.get(WEATHER_DATA_URL)

        if 200 <= response.status_code < 300:
            logging.info('Success! Status code: %s', response.status_code)
        elif 300 <= response.status_code < 400:
            logging.info('Redirection! Status code: %s', response.status_code)
        elif 400 <= response.status_code < 500:
            logging.warning('Client error! Status code: %s',
                            response.status_code)
        elif 500 <= response.status_code < 600:
            logging.error('Server error! Status code: %s',
                          response.status_code)

        return response

    except requests.exceptions.RequestException as e:
        logging.info('Request failed: %s', e)
        return None


def data_transform():
    """Convert API JSON to pandas DataFrame."""
    weather_data = []
    data = get_weather_data()

    if data is None:
        print('No Data!')
        return pd.DataFrame()

    data_json = data.json()
    for entry in data_json.get('list', []):
        dt = datetime.fromtimestamp(entry['dt'], timezone.utc)
        main = entry['main']

        weather_data.append({
            'date': dt,
            'humidity': main['humidity'],
            'temperature': main['temp'],
            'feels_like': main['feels_like'],
            'temperature_max': main['temp_max'],
            'temperature_min': main['temp_min'],
            'pressure': main['pressure'],
            'sea_level': main['sea_level'],
            'ground_level': main['grnd_level'],
            'temperature_correction_factor': main['temp_kf']
        })

    return pd.DataFrame(weather_data)


def connect_to_db():
    """Create SQLAlchemy engine using DB config."""
    return sqlalchemy.create_engine(config.Config_Database.database_url())


def write_to_db(fetched_at):
    """Add timestamp and write DataFrame to DB."""
    df = data_transform()
    df['fetched_at'] = fetched_at

    engine = connect_to_db()
    with engine.begin() as conn:
        df.to_sql('weather_data_forecast', conn,
                  if_exists='append', index=False)
    engine.dispose()


def run():
    """Main entry point."""
    fetched_at = datetime.now(timezone.utc)
    write_to_db(fetched_at)


if __name__ == '__main__':
    run()
