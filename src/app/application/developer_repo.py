from abc import abstractmethod
from typing import Any, Protocol

from app.domain.developer import Developer


class DeveloperRepository(Protocol):
    """Abstract repository for Developer"""

    @abstractmethod
    async def add(self, developer: Developer) -> None:
        """Add developer to repository"""
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, developer_id: str) -> Developer | None:
        """Get developer by ID"""
        raise NotImplementedError

    @abstractmethod
    async def get_all(
        self,
        filter_query: dict[str, Any] | None = None,
        skip: int = 0,
        limit: int = 0,
        sort: list[tuple[str, int]] | None = None,
    ) -> list[Developer]:
        """Get all developers with filtering"""
        raise NotImplementedError

    @abstractmethod
    async def delete(self, developer_id: str) -> None:
        """Delete developer"""
        raise NotImplementedError
