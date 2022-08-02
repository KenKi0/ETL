from typing import Any, Protocol


class Storage(Protocol):
    def save_state(self, state: dict) -> None:
        ...

    def retrieve_state(self) -> dict:
        ...


class State:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.state = self.retrieve_state()

    def retrieve_state(self) -> dict:
        data = self.storage.retrieve_state()
        if not data:
            return {}
        return data

    def set_state(self, key: str, value: Any) -> None:
        self.state[key] = value

        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        return self.state.get(key)
