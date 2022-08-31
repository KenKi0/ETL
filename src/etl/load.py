""" Load parts logic"""
import json
from pathlib import Path
from typing import Iterator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from etl.transform import Transform


class ElasticLoader:
    def __init__(
        self,
        transform: Transform,
        elastic: Elasticsearch,
        index_name: dict,
        index_scheme_path: dict[str, Path],
        batch_size: int,
    ) -> None:
        self.elastic = elastic
        self.transform = transform
        self.index_names = index_name
        self.index_scheme_path = index_scheme_path
        self.batch_size = batch_size
        self.data = self.transform.transform()
        self.index_exist()
        self.is_loaded = False

    def index_exist(self) -> None:
        """
        Check that index is exists if not call self.create_index()
        """
        for name, index_name in self.index_names.items():
            if not self.elastic.indices.exists(index=index_name):
                self.create_index(index_name, name)

    def create_index(self, index_name, path_name) -> None:
        """
        Create elasticsearch index from specified scheme file
        """
        with open(self.index_scheme_path.get(path_name), 'r') as file:
            index_scheme = json.load(file)
            self.elastic.indices.create(index=index_name, body=index_scheme)

    def _prepare_chunked_actions(self, actions: Iterator[dict]) -> Iterator[list[dict]]:
        chunk = []
        while cur_data := next(actions):
            chunk.append(cur_data)
            if len(chunk) == self.batch_size:
                yield chunk
                chunk = []
        if chunk:
            self.is_loaded = True
            yield chunk
        yield None

    def load(self) -> None:
        """
        Main loader function that load transformed data to self.index
        """
        for cur_data in self.data.values():
            chunked_actions = self._prepare_chunked_actions(cur_data)
            while actions := next(chunked_actions):
                bulk(client=self.elastic, actions=actions)
