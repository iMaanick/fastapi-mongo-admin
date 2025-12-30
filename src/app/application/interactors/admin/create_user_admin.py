import logging
from dataclasses import dataclass
from typing import Any

from adaptix import Retort

from app.application.change_tracker import ChangeTracker
from app.application.user_repo import UserRepository
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class CreateUserAdminRequest:
    data: dict[str, Any]


@dataclass(slots=True, frozen=True)
class CreateUserAdminInteractor:
    user_repository: UserRepository
    change_tracker: ChangeTracker
    retort: Retort

    async def __call__(
        self,
        request_data: CreateUserAdminRequest,
    ) -> User:
        """Создать нового пользователя через админку"""
        logger.info(
            "Admin: Creating new user with data: %s",
            request_data.data,
        )

        user = self.retort.load(request_data.data, User)

        await self.user_repository.add(user)
        await self.change_tracker.commit()

        logger.info(
            "Admin: User created: %s (id=%s)",
            user.username,
            user._id,  # noqa: SLF001
        )
        return user
