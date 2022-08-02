import json
from typing import Optional


class FilePathNotSpecifiedError(Exception):
    def __str__(self):
        return 'The file path was not specified, specify file path!'


class JsonFileStorage:
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        if self.file_path is None:
            raise FilePathNotSpecifiedError

        with open(self.file_path, 'w') as f:
            json.dump(state, f)

    def retrieve_state(self) -> dict:
        if self.file_path is None:
            return {}

        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)
                if not data:
                    return {}

            return data

        except FileNotFoundError:
            self.save_state({})
            return {}
