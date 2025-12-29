import logging
from abc import abstractmethod
from typing import Protocol

from app.domain.model import User

logger = logging.getLogger(__name__)


class ChangeTracker(Protocol):
    @abstractmethod
    def track(self, user: User) -> None:
        raise NotImplementedError

    @abstractmethod
    def track_all(self, users: list[User]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def save(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError
