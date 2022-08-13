""" Transform parts logic"""
import json
import logging
from typing import Iterator, Protocol

import scheme
from src.config import settings
from src.etl.extract import Extracting
from src.utils import TransformedData

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
        while films := next(films_data):
            serialized_films = [scheme.FilmScheme(**film) for film in films]
            bulk_data = []
            for film in serialized_films:
                index = {
                    'index': {
                        '_index': settings.elastic.INDEX,
                        '_id': str(film.id),
                    },
                }
                bulk_data.append(json.dumps(index))
                bulk_data.append(film.json())
            bulk_data = '\n'.join(bulk_data) + '\n'
            yield bulk_data

        yield None

    def _transform_persons(self) -> Iterator[dict]:
        """
        Generator to transform persons data
        """
        persons_data = self.data['persons']
        while persons := next(persons_data):
            serialized_persons = [scheme.PersonScheme(**person) for person in persons]
            es_actions = [
                {
                    '_op_type': 'update',
                    '_index': settings.elastic.INDEX,
                    '_id': person.film_id,
                    '_source': {
                        'doc': {
                            'director': person.director,
                            'actors': person.actors,
                            'writers': person.writers,
                            'actors_names': person.actors_names,
                            'writers_names': person.writers_names,
                        },
                    },
                }
                for person in serialized_persons
            ]
            yield es_actions

        yield None

    def _transform_genres(self) -> Iterator[dict]:
        """
        Generator to transform genres data
        """
        genres_data = self.data['genres']
        while genres := next(genres_data):
            serialized_genres = [scheme.GenreScheme(**genre) for genre in genres]
            es_actions = [
                {
                    '_op_type': 'update',
                    '_index': settings.elastic.INDEX,
                    '_id': genre.film_id,
                    '_source': {
                        'doc': {
                            'genres': genre.genres,
                        },
                    },
                }
                for genre in serialized_genres
            ]
            yield es_actions

        yield None
