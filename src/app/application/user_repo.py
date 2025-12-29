from abc import abstractmethod
from typing import Any, Protocol

from app.domain.model import User


class UserRepository(Protocol):
    @abstractmethod
    async def add(self, user: User) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, user_id: str) -> User | None:
        raise NotImplementedError

    @abstractmethod
    async def get_all(
            self,
            filter_query: dict[str, Any] | None = None,
            skip: int = 0,
            limit: int = 0,
            sort: list[tuple[str, int]] | None = None,
    ) -> list[User]:
        raise NotImplementedError


    @abstractmethod
    async def delete(self, user_id: str) -> None:
        raise NotImplementedError
