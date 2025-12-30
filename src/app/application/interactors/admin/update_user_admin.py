import logging
from dataclasses import dataclass
from typing import Any

from adaptix import Retort

from app.application.change_tracker import ChangeTracker
from app.application.exceptions.base import EntityNotFoundError
from app.application.user_repo import UserRepository
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class UpdateUserAdminRequest:
    user_id: str
    data: dict[str, Any]


@dataclass(slots=True, frozen=True)
class UpdateUserAdminInteractor:
    user_repository: UserRepository
    change_tracker: ChangeTracker
    retort: Retort

    async def __call__(self, request_data: UpdateUserAdminRequest) -> User:
        """Обновить пользователя через админку"""
        logger.info("Admin: Updating user: %s", request_data.user_id)

        existing_user = await self.user_repository.get_by_id(
            request_data.user_id,
        )

        if existing_user is None:
            logger.warning("Admin: User not found: %s", request_data.user_id)
            raise EntityNotFoundError(
                entity_type=User,
                field_name="_id",
                field_value=request_data.user_id,
            )

        request_data.data["_id"] = request_data.user_id
        updated_user = self.retort.load(request_data.data, User)

        self.change_tracker.track(updated_user)

        modified_count = await self.change_tracker.save()
        await self.change_tracker.commit()

        logger.info(
            "Admin: User updated: %s, modified_count=%s",
            updated_user.username,
            modified_count,
        )
        return updated_user
