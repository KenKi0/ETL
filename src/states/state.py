from typing import Any, Protocol

from src.states.state_storage import Storage


class BaseState(Protocol):
    def set_state(self, key: str, value: Any) -> None:
        ...

    def get_state(self, key: str) -> Any:
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
