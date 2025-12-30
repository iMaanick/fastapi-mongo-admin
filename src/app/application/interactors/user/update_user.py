import logging
from dataclasses import dataclass

from app.application.change_tracker import ChangeTracker
from app.application.exceptions.base import EntityNotFoundError
from app.application.user_repo import UserRepository
from app.domain.model import Tag, User

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class UpdateUserRequest:
    user_id: str
    username: str | None
    email: str | None
    tags: list[Tag] | None
    is_active: bool | None


@dataclass(slots=True, frozen=True)
class UpdateUserInteractor:
    user_repository: UserRepository
    change_tracker: ChangeTracker

    async def __call__(self, request_data: UpdateUserRequest) -> User:
        logger.info("Updating user: %s", request_data.user_id)

        user = await self.user_repository.get_by_id(request_data.user_id)

        if user is None:
            logger.warning("User not found: %s", request_data.user_id)
            raise EntityNotFoundError(
                entity_type=User,
                field_name="_id",
                field_value=request_data.user_id,
            )

        self.change_tracker.track(user)

        if request_data.username is not None:
            user.username = request_data.username

        if request_data.email is not None:
            user.email = request_data.email

        if request_data.tags is not None:
            user.tags = request_data.tags

        if request_data.is_active is not None:
            user.is_active = request_data.is_active

        modified_count = await self.change_tracker.save()

        await self.change_tracker.commit()
        logger.info(
            "User updated: %s, modified_count=%s",
            user.username,
            modified_count,
        )
        return user
