import logging
from dataclasses import dataclass

from app.application.change_tracker import ChangeTracker
from app.application.user_repo import UserRepository
from app.domain.model import Tag

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class UpdateUsersResponse:
    modified_count: int


@dataclass(slots=True, frozen=True)
class UpdateUsersInteractor:
    user_repository: UserRepository
    change_tracker: ChangeTracker

    async def __call__(self ) -> UpdateUsersResponse:
        logger.info("Starting bulk user update")

        users = await self.user_repository.get_all()

        self.change_tracker.track_all(users)

        for user in users:
            new_tag = Tag(name="updated43434")
            if new_tag not in user.tags:
                user.tags.append(new_tag)

        modified_count = await self.change_tracker.save()

        logger.info(f"Modified {modified_count} users")

        return UpdateUsersResponse(modified_count=modified_count)
