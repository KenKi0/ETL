import uuid
from typing import Optional

import orjson
from pydantic import BaseModel


def orjson_dumps(value, *, default):
    return orjson.dumps(value, default=default).decode()


class BaseScheme(BaseModel):
    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


class Person(BaseScheme):
    id: uuid.UUID  # noqa: VNE003
    full_name: str
    role: list[str]
    film_ids: list[uuid.UUID]


class Genre(BaseScheme):
    id: uuid.UUID  # noqa: VNE003
    name: str
    description: Optional[str]


class FilmScheme(BaseScheme):
    id: uuid.UUID  # noqa: VNE003
    title: str
    description: Optional[str]
    imdb_rating: Optional[float] = 0
    director: Optional[list[dict]]
    actors: Optional[list[dict]] = []
    writers: Optional[list[dict]]
    actors_names: Optional[list[str]]
    writers_names: Optional[list[str]]
    genre: Optional[list[dict]]


class PersonScheme(BaseScheme):
    film_id: uuid.UUID
    director: Optional[list[dict]]
    actors: Optional[list[dict]]
    writers: Optional[list[dict]]
    actors_names: Optional[list[str]]
    writers_names: Optional[list[str]]


class GenreScheme(BaseScheme):
    film_id: uuid.UUID
    genre: Optional[list[dict]]
