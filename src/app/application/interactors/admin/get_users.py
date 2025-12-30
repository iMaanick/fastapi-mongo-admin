import logging
from dataclasses import dataclass
from typing import Any

from app.application.user_repo import UserRepository
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class GetUsersAdminRequest:
    filter_query: dict[str, Any] | None = None
    skip: int = 0
    limit: int = 100
    sort: list[tuple[str, int]] | None = None


@dataclass(slots=True, frozen=True)
class GetUsersAdminInteractor:
    user_repository: UserRepository

    async def __call__(self, request_data: GetUsersAdminRequest) -> list[User]:
        """Получить список пользователей для админки"""
        logger.info(
            "Admin: Getting users: skip=%s, limit=%s",
            request_data.skip,
            request_data.limit,
        )

        users = await self.user_repository.get_all(
            filter_query=request_data.filter_query,
            skip=request_data.skip,
            limit=max(0, request_data.limit),
            sort=request_data.sort,
        )

        logger.info("Admin: Found %s users", len(users))
        return users
