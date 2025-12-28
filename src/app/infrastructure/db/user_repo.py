import logging
from dataclasses import dataclass
from typing import Any

from adaptix import Retort
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection

from app.application.user_repo import UserRepository
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class MongoUserRepository(UserRepository):
    collection: AsyncIOMotorCollection[dict[str, Any]]
    retort: Retort

    async def add(self, user: User) -> None:
        user_dict = self.retort.dump(user)
        user_dict.pop("_id", None)

        result = await self.collection.insert_one(user_dict)
        logger.info(f"User added with ID: {result.inserted_id}")

    async def get_by_id(self, user_id: str) -> User | None:
        try:
            object_id = ObjectId(user_id)
        except Exception:
            logger.warning(f"Invalid ObjectId format: {user_id}")
            return None

        user_doc = await self.collection.find_one({"_id": object_id})

        if not user_doc:
            logger.info(f"User not found: {user_id}")
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
            sort: Список кортежей (field, direction), где direction: 1=asc, -1=desc
        """
        query = filter_query or {}

        cursor = self.collection.find(query)

        if sort:
            cursor = cursor.sort(sort)

        if skip > 0:
            cursor = cursor.skip(skip)

        if limit > 0:
            cursor = cursor.limit(limit)

        user_docs = await cursor.to_list(length=None)
        users = self.retort.load(user_docs, list[User])

        logger.info(f"Loaded {len(users)} users with filter: {query}")
        return users

    async def delete(self, user_id: str) -> None:
        try:
            object_id = ObjectId(user_id)
        except Exception:
            logger.warning(f"Invalid ObjectId format: {user_id}")
            raise ValueError(f"Invalid user ID format: {user_id}")

        result = await self.collection.delete_one({"_id": object_id})

        if result.deleted_count == 0:
            logger.warning(f"User not found for deletion: {user_id}")
            raise ValueError(f"User not found: {user_id}")

        logger.info(f"User deleted: {user_id}")
