import logging
from dataclasses import dataclass

from app.application.user_repo import UserRepository
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class GetUserByIdAdminRequest:
    user_id: str


@dataclass(slots=True, frozen=True)
class GetUserByIdAdminInteractor:
    user_repository: UserRepository

    async def __call__(
        self,
        request_data: GetUserByIdAdminRequest,
    ) -> User | None:
        """Получить пользователя по ID для админки"""
        logger.info("Admin: Getting user by id: %s", request_data.user_id)

        user = await self.user_repository.get_by_id(request_data.user_id)

        if user is None:
            logger.warning("Admin: User not found: %s", request_data.user_id)
        else:
            logger.info("Admin: User found: %s", user.username)

        return user
