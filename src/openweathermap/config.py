import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()


class Config_Api:
    """API configuration."""
    WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')  # OpenWeatherMap API key


class Config_Database:
    """Database configuration."""
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    @classmethod
    def database_url(cls):
        """Return full SQLAlchemy database URL."""
        return (f'postgresql+psycopg2://{cls.DB_USERNAME}:{cls.DB_PASSWORD}@'
                f'{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}')
