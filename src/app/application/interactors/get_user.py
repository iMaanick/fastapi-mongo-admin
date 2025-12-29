import logging
from dataclasses import dataclass

from app.application.user_repo import UserRepository
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class GetUserRequest:
    user_id: str


@dataclass(slots=True, frozen=True)
class GetUserInteractor:
    user_repository: UserRepository

    async def __call__(self, request_data: GetUserRequest) -> User | None:
        logger.info("Getting user by ID: %s", request_data.user_id)

        user = await self.user_repository.get_by_id(request_data.user_id)

        if user is None:
            logger.info("User not found: %s", request_data.user_id)
            return None

        logger.info("User found: %s", user.username)

        return user
