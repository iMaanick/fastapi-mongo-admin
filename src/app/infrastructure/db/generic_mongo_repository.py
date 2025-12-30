import logging
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from adaptix import Retort
from bson import ObjectId
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(slots=True, frozen=True)
class GenericMongoRepository(Generic[T]):
    """Generic repository для работы с MongoDB"""

    client: AsyncIOMotorClient[dict[str, Any]]
    database_name: str
    collection_name: str
    model_type: type[T]
    retort: Retort

    @property
    def database(self) -> AsyncIOMotorDatabase[dict[str, Any]]:
        return self.client[self.database_name]

    @property
    def collection(self) -> AsyncIOMotorCollection[dict[str, Any]]:
        return self.database[self.collection_name]

    async def add(
        self,
        entity: T,
        session: AsyncIOMotorClientSession,
    ) -> None:
        """Добавить новую сущность"""
        entity_dict = self.retort.dump(entity)
        entity_dict.pop("_id", None)

        result = await self.collection.insert_one(
            entity_dict,
            session=session,
        )
        logger.info(
            "%s added with ID: %s",
            self.model_type.__name__,
            result.inserted_id,
        )

    async def get_by_id(
        self,
        entity_id: str,
        session: AsyncIOMotorClientSession,
    ) -> T | None:
        """Получить сущность по ID"""
        object_id = ObjectId(entity_id)

        doc = await self.collection.find_one(
            {"_id": object_id},
            session=session,
        )

        if not doc:
            logger.info(
                "%s not found: %s",
                self.model_type.__name__,
                entity_id,
            )
            return None

        return self.retort.load(doc, self.model_type)

    async def get_all(
        self,
        filter_query: dict[str, Any] | None = None,
        skip: int = 0,
        limit: int = 0,
        sort: list[tuple[str, int]] | None = None,
        session: AsyncIOMotorClientSession | None = None,
    ) -> list[T]:
        """Получить список сущностей с фильтрацией и сортировкой"""
        query = filter_query or {}

        cursor = self.collection.find(query, session=session)

        if sort:
            cursor = cursor.sort(sort)

        if skip > 0:
            cursor = cursor.skip(skip)

        if limit > 0:
            cursor = cursor.limit(limit)

        docs = await cursor.to_list(length=None)
        entities = self.retort.load(docs, list[self.model_type])

        logger.info(
            "Loaded %s %s with filter: %s",
            len(entities),
            self.model_type.__name__,
            query,
        )
        return entities

    async def update(
        self,
        entity_id: str,
        entity: T,
        session: AsyncIOMotorClientSession,
    ) -> bool:
        """Обновить сущность"""
        object_id = ObjectId(entity_id)

        entity_dict = self.retort.dump(entity)
        entity_dict.pop("_id", None)

        result = await self.collection.replace_one(
            {"_id": object_id},
            entity_dict,
            session=session,
        )

        if result.matched_count == 0:
            logger.warning(
                "%s not found for update: %s",
                self.model_type.__name__,
                entity_id,
            )
            return False

        logger.info("%s updated: %s", self.model_type.__name__, entity_id)
        return True

    async def delete(
        self,
        entity_id: str,
        session: AsyncIOMotorClientSession,
    ) -> bool:
        """Удалить сущность"""
        object_id = ObjectId(entity_id)

        result = await self.collection.delete_one(
            {"_id": object_id},
            session=session,
        )

        if result.deleted_count == 0:
            logger.warning(
                "%s not found for deletion: %s",
                self.model_type.__name__,
                entity_id,
            )
            return False

        logger.info("%s deleted: %s", self.model_type.__name__, entity_id)
        return True
