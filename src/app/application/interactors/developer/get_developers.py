import logging
from dataclasses import dataclass

from app.application.developer_repo import DeveloperRepository
from app.domain.developer import Developer

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class GetDevelopersInteractor:
    developer_repository: DeveloperRepository

    async def __call__(
        self,
    ) -> list[Developer]:
        logger.info(
            "Getting developers",
        )

        developers = await self.developer_repository.get_all()

        logger.info(
            "%s developers retrieved",
            len(developers),
        )

        return developers
