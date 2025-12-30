import logging
from dataclasses import dataclass
from typing import Any

from app.application.user_repo import UserRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class CountUsersAdminRequest:
    filter_query: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class CountUsersAdminInteractor:
    user_repository: UserRepository

    async def __call__(self, request_data: CountUsersAdminRequest) -> int:
        """Подсчитать количество пользователей для админки"""
        logger.info(
            "Admin: Counting users with filter: %s",
            request_data.filter_query,
        )

        users = await self.user_repository.get_all(
            filter_query=request_data.filter_query,
            skip=0,
            limit=0,
            sort=None,
        )

        count = len(users)
        logger.info("Admin: User count: %s", count)
        return count
