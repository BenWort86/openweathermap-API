
from datetime import datetime, timezone  # timestamps with timezone
import pandas as pd  # DataFrames
from . import weather_data_config as cfg  # API keys and DB config
from . import weather_data_common as common


# OpenWeatherMap current weather API for Hamburg
weather_data_url = (
    f'https://api.openweathermap.org/data/2.5/weather?lat=53.551086&'
    f'lon=9.993682&appid={cfg.Config_Api.WEATHER_API_KEY}&units=metric'
)

logger = common.get_logger('weather_actual')

response = common.get_weather_data(weather_data_url, logger)


def data_transform():
    """Convert API JSON to pandas DataFrame."""
    if response is None:
        print('No Data!')
        return pd.DataFrame()

    data_json = response.json()
    main = data_json['main']
    dt = datetime.fromtimestamp(data_json['dt'], timezone.utc)

    # Flatten main weather metrics into a dict
    weather_data = [{
        'date': dt,
        'humidity': main.get('humidity'),
        'temperature': main.get('temp'),
        'feels_like': main.get('feels_like'),
        'temperature_max': main.get('temp_max'),
        'temperature_min': main.get('temp_min'),
        'pressure': main.get('pressure'),
        'sea_level': main.get('sea_level'),
        'ground_level': main.get('grnd_level'),
    }]

    return pd.DataFrame(weather_data)


def run():
    """Main entry point."""
    weather_data_df = data_transform()
    fetched_at = datetime.now(timezone.utc)
    common.write_to_db(fetched_at, weather_data_df, 'weather_data_actual')


if __name__ == '__main__':
    run()
