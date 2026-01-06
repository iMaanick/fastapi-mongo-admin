import logging
from dataclasses import dataclass

from app.application.developer_repo import DeveloperRepository
from app.domain.developer import Developer

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class GetDeveloperRequest:
    developer_id: str


@dataclass(slots=True, frozen=True)
class GetDeveloperInteractor:
    developer_repository: DeveloperRepository

    async def __call__(
        self,
        request_data: GetDeveloperRequest,
    ) -> Developer | None:
        logger.info("Getting developer: %s", request_data.developer_id)

        developer = await self.developer_repository.get_by_id(
            request_data.developer_id,
        )

        if developer is None:
            logger.info("Developer not found: %s", request_data.developer_id)
            return None

        logger.info(
            "Developer retrieved: %s (ID: %s)",
            developer.username,
            developer._id,  # noqa: SLF001
        )

        return developer
