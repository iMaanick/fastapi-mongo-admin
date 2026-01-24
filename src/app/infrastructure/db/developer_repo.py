import logging
from dataclasses import dataclass
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import (
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

from app.application.developer_repo import DeveloperRepository
from app.domain.developer import Developer
from app.infrastructure.trackers.mongo_session import MongoSession

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class MongoDeveloperRepository(DeveloperRepository):
    database: AsyncIOMotorDatabase[dict[str, Any]]
    session: AsyncIOMotorClientSession
    mongo_session: MongoSession

    @property
    def collection(self) -> AsyncIOMotorCollection[dict[str, Any]]:
        return self.database["developers"]

    async def add(self, developer: Developer) -> None:
        self.mongo_session.add(developer)

        logger.info("Developer added")

    async def get_by_id(self, developer_id: str) -> Developer | None:
        object_id = ObjectId(developer_id)

        developer = await self.mongo_session.find_one(
            Developer,
            self.collection,
            {"_id": object_id},
        )

        if not developer:
            logger.info("Developer not found: %s", developer_id)
            return None

        return developer

    async def get_all(
        self,
        filter_query: dict[str, Any] | None = None,
        skip: int = 0,
        limit: int = 0,
        sort: list[tuple[str, int]] | None = None,
    ) -> list[Developer]:
        """Get developers with filtering, pagination and sorting"""
        query = filter_query or {}

        cursor = self.collection.find(query, session=self.session)

        if sort:
            cursor = cursor.sort(sort)

        if skip > 0:
            cursor = cursor.skip(skip)

        if limit > 0:
            cursor = cursor.limit(limit)

        developers = await self.mongo_session.find_all(Developer, cursor, length=None)

        logger.info(
            "Loaded %s developers with filter: %s",
            len(developers),
            query,
        )
        return developers

    async def delete(self, developer_id: str) -> None:

        developer = await self.get_by_id(developer_id)

        if developer is None:
            logger.warning(
                "Developer not found for deletion: %s",
                developer_id,
            )
            return

        self.mongo_session.delete(developer)
