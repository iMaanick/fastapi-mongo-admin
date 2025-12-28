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

    async def get_all(self) -> list[User]:
        cursor = self.collection.find({})

        user_docs = await cursor.to_list(length=None)

        users = self.retort.load(user_docs, list[User])

        logger.info(f"Loaded {len(users)} users")
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
