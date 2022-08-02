import argparse
import logging.config

import psycopg2
from config import settings
from elasticsearch import Elasticsearch
from extract import PartName, PostgresExtracting
from load import ElasticLoader
from psycopg2.extras import DictCursor
from state import State
from state_storage import JsonFileStorage
from transform import ElasticTransformer
from utils import Backoff, db_conn

logging.config.dictConfig(settings.LOG_CONFIG)
logger = logging.getLogger(__name__)


@Backoff()
def start_etl_process(
    parts_to_extract: list[PartName],
    batch_size: int,
) -> None:
    state = State(JsonFileStorage('./state.json'))

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
    args = parser.parse_args()

    if args.init:
        parts = [PartName.films]
    else:
        parts = [PartName.films, PartName.persons, PartName.genres]

    start_etl_process(parts, args.batch_size)
