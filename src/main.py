import argparse
import logging.config
from time import sleep

import psycopg2
from psycopg2.extras import DictCursor
from elasticsearch import Elasticsearch

from config import settings
from src.etl.extract import PartName, PostgresExtracting
from src.etl.load import ElasticLoader
from src.states.state import State
from src.states.state_storage import JsonFileStorage
from src.etl.transform import ElasticTransformer
from utils import Backoff, db_conn

logging.config.dictConfig(settings.LOG_CONFIG)
logger = logging.getLogger(__name__)


@Backoff()
def start_etl_process(
    parts_to_extract: list[PartName],
    batch_size: int,
) -> None:
    state = State(JsonFileStorage('./data/state.json'))

    with db_conn(psycopg2.connect(**settings.postgres.dsl, cursor_factory=DictCursor)) as conn:
        extract = PostgresExtracting(conn, state, settings.DEFAULT_PROCESS_TIME, parts_to_extract, batch_size)

        transform = ElasticTransformer(extract)

        loader = ElasticLoader(
            transform,
            Elasticsearch(settings.elastic.hosts),
            settings.elastic.INDEX,
            settings.elastic.INDEX_FILE,
        )

        loader.load()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--init',
        help='Start ETL proccess only for films parts',
        action='store_const',
        const=True,
        default=False,
    )
    parser.add_argument('--batch-size', type=int, help='Count loaded data for one iteration of ETL', default=1000)
    parser.add_argument('--freq', type=int, help='How often should the process be performed in minutes', default=10)
    args = parser.parse_args()

    if args.init:
        parts = [PartName.films]
    else:
        parts = [PartName.films, PartName.persons, PartName.genres]

    while True:
        start_etl_process(parts, args.batch_size)
        sleep(args.freq * 60)
