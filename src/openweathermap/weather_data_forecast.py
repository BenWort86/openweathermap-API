from datetime import datetime, timezone  # timestamps with timezone
import pandas as pd  # dataframes
from . import weather_data_config as cfg  # API keys and DB config
from . import weather_data_common as common


# OpenWeatherMap forecast API for Hamburg
weather_data_url = (
    f'https://api.openweathermap.org/data/2.5/forecast?lat=53.551086&'
    f'lon=9.993682&appid={cfg.Config_Api.WEATHER_API_KEY}&'
    f'units=metric'
)

logger = common.get_logger('weather_forecast')

response = common.get_weather_data(weather_data_url, logger)


def data_transform():
    """Convert API JSON to pandas DataFrame."""
    weather_data = []

    if response is None:
        print('No Data!')
        return pd.DataFrame()

    data_json = response.json()
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


def run():
    """Main entry point."""
    weather_data_df = data_transform()
    fetched_at = datetime.now(timezone.utc)
    common.write_to_db(fetched_at, weather_data_df, 'weather_data_forecast')


if __name__ == '__main__':
    run()
