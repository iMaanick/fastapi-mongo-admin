import logging
from dataclasses import dataclass
from typing import Any

from adaptix import Retort
from bson import ObjectId
from motor.motor_asyncio import (
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

from app.application.exceptions.base import EntityNotFoundError
from app.application.user_repo import UserRepository
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class MongoUserRepository(UserRepository):
    database: AsyncIOMotorDatabase[dict[str, Any]]
    retort: Retort
    session: AsyncIOMotorClientSession

    @property
    def collection(self) -> AsyncIOMotorCollection[dict[str, Any]]:
        return self.database["example"]

    async def add(self, user: User) -> None:
        user_dict = self.retort.dump(user)
        user_dict.pop("_id", None)

        result = await self.collection.insert_one(
            user_dict,
            session=self.session,
        )
        logger.info("User added with ID: %s", result.inserted_id)

    async def get_by_id(self, user_id: str) -> User | None:
        object_id = ObjectId(user_id)

        user_doc = await self.collection.find_one(
            {"_id": object_id},
            session=self.session,
        )

        if not user_doc:
            logger.info("User not found: %s", user_id)
            return None

        return self.retort.load(user_doc, User)

    async def get_all(
        self,
        filter_query: dict[str, Any] | None = None,
        skip: int = 0,
        limit: int = 0,
        sort: list[tuple[str, int]] | None = None,
    ) -> list[User]:
        """
        Получить пользователей с фильтрацией, пагинацией и сортировкой

        Args:
            filter_query: MongoDB filter (например {"is_active": True})
            skip: Количество документов для пропуска
            limit: Максимальное количество документов (0 = без лимита)
            sort: Список кортежей (field, direction),
            где direction: 1=asc, -1=desc
        """
        query = filter_query or {}

        cursor = self.collection.find(query, session=self.session)

        if sort:
            cursor = cursor.sort(sort)

        if skip > 0:
            cursor = cursor.skip(skip)

        if limit > 0:
            cursor = cursor.limit(limit)

        user_docs = await cursor.to_list(length=None)
        users = self.retort.load(user_docs, list[User])

        logger.info("Loaded %s users with filter: %s", len(users), query)
        return users

    async def delete(self, user_id: str) -> None:
        object_id = ObjectId(user_id)

        result = await self.collection.delete_one(
            {"_id": object_id},
            session=self.session,
        )

        if result.deleted_count == 0:
            logger.warning("User not found for deletion: %s", user_id)
            raise EntityNotFoundError(
                entity_type=User,
                field_name="_id",
                field_value=user_id,
            )

        logger.info("User deleted: %s", user_id)
