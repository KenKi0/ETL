""" Load parts logic"""
import json
from pathlib import Path
from typing import Protocol

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from utils import TransformedData


class Transform(Protocol):
    def transform(self) -> TransformedData:
        ...


class ElasticLoader:
    def __init__(
        self,
        transform: Transform,
        elastic: Elasticsearch,
        index_name: str,
        index_scheme_path: Path,
    ) -> None:
        self.elastic = elastic
        self.transform = transform
        self.index_name = index_name
        self.index_scheme_path = index_scheme_path
        self.data = self.transform.transform()
        self.index_exist()

    def index_exist(self) -> None:
        """
        Check that index is exists if not call self.create_index()
        """
        if not self.elastic.indices.exists(index=self.index_name):
            self.create_index()

    def create_index(self) -> None:
        """
        Create elasticsearch index from specified scheme file
        """
        with open(self.index_scheme_path, 'r') as file:
            index_scheme = json.load(file)
            self.elastic.indices.create(index=self.index_name, body=index_scheme)

    def load(self) -> None:
        """
        Main loader function that load transformed data to self.index
        """
        if cur_data := self.data.get('films'):
            while bulk_data := next(cur_data):
                self.elastic.bulk(operations=bulk_data, index=self.index_name)
        self.data.pop('films')
        for cur_data in self.data.values():
            while actions := next(cur_data):
                bulk(self.elastic, actions)
