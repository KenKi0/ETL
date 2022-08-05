import datetime
from pathlib import Path

from pydantic import BaseSettings

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}


class BaseConfig(BaseSettings):
    class Config:
        env_file = Path(Path(__file__).parent.parent, '.env')
        env_file_encoding = 'utf-8'


class PostgresSettings(BaseConfig):
    USER: str
    PASSWORD: str
    DB: str
    HOST: str
    PORT: str = '5432'

    @property
    def uri(self):
        return f'postgresql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DB}'

    @property
    def dsl(self):
        return {
            'dbname': self.DB,
            'user': self.USER,
            'password': self.PASSWORD,
            'host': self.HOST,
            'port': self.PORT,
        }

    class Config:
        env_prefix = 'PG_'


class ElasticSettings(BaseConfig):
    HOST: str
    PORT: str = '9200'
    INDEX: str = 'movies'
    INDEX_FILE: Path = Path(Path(__file__).parent, 'index_schema.json')

    class Config:
        env_prefix = 'ES_'

    @property
    def hosts(self):
        return [{'host': self.HOST, 'port': self.PORT}]


class ProjectSettings(BaseConfig):
    DEFAULT_PROCESS_TIME: datetime.datetime = datetime.datetime(2010, 1, 1)
    LOG_CONFIG: dict = LOGGING_CONFIG

    postgres: PostgresSettings = PostgresSettings()
    elastic: ElasticSettings = ElasticSettings()


settings = ProjectSettings()
