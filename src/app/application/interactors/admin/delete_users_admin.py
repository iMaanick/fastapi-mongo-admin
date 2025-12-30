import logging
from dataclasses import dataclass

from app.application.change_tracker import ChangeTracker
from app.application.user_repo import UserRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class DeleteUsersAdminRequest:
    user_ids: list[str]


@dataclass(slots=True, frozen=True)
class DeleteUsersAdminInteractor:
    user_repository: UserRepository
    change_tracker: ChangeTracker

    async def __call__(self, request_data: DeleteUsersAdminRequest) -> int:
        """Удалить пользователей через админку"""
        logger.info("Admin: Deleting users: %s", request_data.user_ids)

        deleted = 0
        for user_id in request_data.user_ids:
            await self.user_repository.delete(user_id)
            deleted += 1
            logger.debug("Admin: User deleted: %s", user_id)

        await self.change_tracker.commit()

        logger.info("Admin: Total users deleted: %s", deleted)
        return deleted
