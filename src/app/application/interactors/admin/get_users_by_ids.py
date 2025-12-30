import logging
from dataclasses import dataclass

from bson import ObjectId

from app.application.user_repo import UserRepository
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class GetUsersByIdsAdminRequest:
    user_ids: list[str]


@dataclass(slots=True, frozen=True)
class GetUsersByIdsAdminInteractor:
    user_repository: UserRepository

    async def __call__(
        self,
        request_data: GetUsersByIdsAdminRequest,
    ) -> list[User]:
        """Получить пользователей по списку ID для админки"""
        logger.info("Admin: Getting users by ids: %s", request_data.user_ids)

        if not request_data.user_ids:
            logger.info("Admin: No user ids provided, returning empty list")
            return []

        object_ids = [ObjectId(user_id) for user_id in request_data.user_ids]

        users = await self.user_repository.get_all(
            filter_query={"_id": {"$in": object_ids}},
            skip=0,
            limit=0,
            sort=None,
        )

        logger.info(
            "Admin: Found %s users out of %s ids",
            len(users),
            len(request_data.user_ids),
        )
        return users
