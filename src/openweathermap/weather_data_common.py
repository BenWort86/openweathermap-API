
import requests  # HTTP requests
import sqlalchemy  # DB connection
import weather_data_config as cfg  # API keys and DB config
from pathlib import Path  # file paths
import logging  # logging


# Base and log directories
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


def get_logger(name):
    """Return a logger writing to a specific file."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(LOG_DIR / f'{name}.log', mode='a')
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def get_weather_data(weather_data_url, logger):
    """Fetch weather data from API and handle response codes."""
    try:
        response = requests.get(weather_data_url)

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


def connect_to_db():
    """Create SQLAlchemy engine using DB config."""
    return sqlalchemy.create_engine(cfg.Config_Database.database_url())


def write_to_db(fetched_at, weather_data_df, table_name):
    """Add timestamp and write DataFrame to DB."""
    weather_data_df['fetched_at'] = fetched_at

    engine = connect_to_db()
    with engine.begin() as conn:
        weather_data_df.to_sql(table_name, conn,
                               if_exists='append', index=False)
    engine.dispose()
