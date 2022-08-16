""" Transform parts logic"""
import logging
from typing import Iterator, Protocol

from config import settings
from etl import scheme
from etl.extract import Extracting
from utils import TransformedData

logger = logging.getLogger(__name__)


class Transform(Protocol):
    def transform(self) -> TransformedData:
        ...


class ElasticTransformer:
    def __init__(self, extract: Extracting) -> None:
        self.extract = extract
        self.data = self.extract.extract()

    def transform(self) -> TransformedData:
        """
        Main transform function to create dict with transform generators
        :return: dict with generators that return transformed data
        """
        result = {}
        for extract_name in self.data.keys():
            try:
                result[extract_name] = getattr(self, f'_transform_{extract_name}')()
            except AttributeError:
                logger.warning('Non-existent extract part: %s', extract_name)
        return result

    def _transform_films(self) -> Iterator[dict]:
        """
        Generator to transform films data
        """
        films_data = self.data['films']
        while film := next(films_data):
            serialized_film = scheme.FilmScheme(**film)
            es_actions = {
                '_op_type': 'index',
                '_index': settings.elastic.INDEX.get('films'),
                '_id': serialized_film.id,
                '_source': serialized_film.dict(),
            }
            yield es_actions

        yield None

    def _transform_films_persons(self) -> Iterator[dict]:
        """
        Generator to transform film persons data
        """
        persons_data = self.data['films_persons']
        while person := next(persons_data):
            serialized_person = scheme.PersonScheme(**person)
            es_actions = {
                '_op_type': 'update',
                '_index': settings.elastic.INDEX.get('films'),
                '_id': serialized_person.film_id,
                'doc': serialized_person.dict(exclude={'film_id'}),
            }
            yield es_actions

        yield None

    def _transform_films_genres(self) -> Iterator[dict]:
        """
        Generator to transform film genres data
        """
        genres_data = self.data['films_genres']
        while genre := next(genres_data):
            serialized_genre = scheme.GenreScheme(**genre)
            es_action = {
                '_op_type': 'update',
                '_index': settings.elastic.INDEX.get('films'),
                '_id': serialized_genre.film_id,
                'doc': serialized_genre.dict(exclude={'film_id'}),
            }

            yield es_action

        yield None

    def _transform_persons(self) -> Iterator[dict]:
        """
        Generator to transform persons data
        """
        persons_data = self.data['persons']
        while person := next(persons_data):
            serialized_person = scheme.Person(**person)
            es_action = {
                '_op_type': 'index',
                '_index': settings.elastic.INDEX.get('persons'),
                '_id': serialized_person.id,
                '_source': serialized_person.dict(),
            }

            yield es_action

        yield None

    def _transform_genres(self) -> Iterator[dict]:
        """
        Generator to transform genres data
        """
        genres_data = self.data['genres']
        while genre := next(genres_data):
            serialized_genre = scheme.Genre(**genre)
            es_action = {
                '_op_type': 'index',
                '_index': settings.elastic.INDEX.get('genres'),
                '_id': serialized_genre.id,
                '_source': serialized_genre.dict(),
            }

            yield es_action

        yield None
