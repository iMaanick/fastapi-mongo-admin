import logging
from dataclasses import dataclass

from app.application.developer_repo import DeveloperRepository
from app.infrastructure.trackers.mongo_session import MongoSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class DeleteDeveloperRequest:
    developer_id: str


@dataclass(slots=True, frozen=True)
class DeleteDeveloperInteractor:
    developer_repository: DeveloperRepository
    session: MongoSession

    async def __call__(
        self,
        request_data: DeleteDeveloperRequest,
    ) -> bool:
        logger.info("Deleting developer: %s", request_data.developer_id)

        # Delete developer (repository marks for deletion in session)
        await self.developer_repository.delete(request_data.developer_id)

        # Commit the deletion
        await self.session.commit()

        logger.info("Developer deleted: %s", request_data.developer_id)

        return True
