""" Extract parts logic"""
import datetime
import logging
from enum import Enum
from typing import Iterator, Protocol

from src import raw_sql
from psycopg2 import extensions as pg_ext
from psycopg2.extras import DictRow

from src.states.state import BaseState
from src.utils import DatabaseData

logger = logging.getLogger(__name__)


class Extracting(Protocol):
    def extract(self) -> DatabaseData:
        ...


class PartName(Enum):
    films: str = 'films'
    persons: str = 'persons'
    genres: str = 'genres'


class PostgresExtracting:
    def __init__(
        self,
        conn: pg_ext.connection,
        state: BaseState,
        default_process_time: datetime.datetime,
        extract_parts: list[PartName],
        extract_size: int,
    ) -> None:
        self.conn = conn
        self.cur: pg_ext.cursor = conn.cursor()
        self.state = state
        self.extract_size = extract_size
        self.extract_parts = extract_parts
        self.default_process_time = default_process_time

    def extract(self) -> DatabaseData:
        """
        Main extracting function to create dict with extracting generators
        :return: dict with generators that return data from database
        """
        result = {}
        for extract_name in self.extract_parts:
            try:
                result[extract_name.value] = getattr(self, f'_extract_{extract_name.value}')()
            except AttributeError:
                logger.warning('Non-existent extract part: %s', extract_name.value)
        return result

    def _get_process_last_filed_time(self, process_name: str) -> str:
        """
        Function that return last extracted data modified field value
        :param process_name: name of process that kept in state
        :return: time in string format
        """
        if (time := self.state.get_state(process_name)) is not None:
            return time
        return str(self.default_process_time)

    def _extract_films(self) -> Iterator[list[DictRow]]:
        """
        Generator to extract films data
        """
        last_extracted_time = self._get_process_last_filed_time('films')

        self.cur.execute(raw_sql.film, [last_extracted_time])

        while films := self.cur.fetchmany(self.extract_size):
            yield films
            self.state.set_state('films', str(films[-1].get('updated_at')))

        yield None

    def _extract_persons(self) -> Iterator[list[DictRow]]:
        """
        Generator to extract persons data
        """
        last_extracted_time = self._get_process_last_filed_time('persons')
        self.cur.execute(raw_sql.person_id, [last_extracted_time])
        persons = self.cur.fetchall()
        persons_id = [person.get('id') for person in persons]

        if persons_id:

            last_extracted_time = self._get_process_last_filed_time('persons_film')
            self.cur.execute(raw_sql.person_film_id, [tuple(persons_id), last_extracted_time])

            while films := self.cur.fetchmany(self.extract_size):
                films_ids = [film.get('id') for film in films]
                self.cur.execute(raw_sql.person_films, [tuple(films_ids)])

                yield self.cur.fetchall()

                self.state.set_state('persons_film', str(films[-1].get('updated_at')))

            self.state.set_state('persons', str(persons[-1].get('updated_at')))

        yield None

    def _extract_genres(self) -> Iterator[list[DictRow]]:
        """
        Generator to extract persons data
        """
        last_extracted_time = self._get_process_last_filed_time('genres')
        self.cur.execute(raw_sql.genre_id, [last_extracted_time])
        genres = self.cur.fetchall()
        genres_id = [genre.get('id') for genre in genres]

        if genres_id:

            last_extracted_time = self._get_process_last_filed_time('genres_film')
            self.cur.execute(raw_sql.genre_film_id, [tuple(genres_id), last_extracted_time])

            while films := self.cur.fetchmany(self.extract_size):
                films_ids = [film.get('id') for film in films]
                self.cur.execute(raw_sql.genre_films, [tuple(films_ids)])

                yield self.cur.fetchall()

                self.state.set_state('genres_film', str(films[-1].get('updated_at')))

            self.state.set_state('genres', str(genres[-1].get('updated_at')))

        yield None
