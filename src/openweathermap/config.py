
import os
from dotenv import load_dotenv

load_dotenv()


class Config_Api:
    WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')


class Config_Database:

    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    @classmethod
    def database_url(cls):
        return (f'postgresql+psycopg2://{cls.DB_USERNAME}:{cls.DB_PASSWORD}@'
                f'{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}')
