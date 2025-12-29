import logging
from dataclasses import dataclass, field
from typing import Any

from adaptix import Retort
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorClientSession
from pymongo import ReplaceOne

from app.application.change_tracker import ChangeTracker
from app.domain.model import User

logger = logging.getLogger(__name__)


@dataclass
class MongoChangeTracker(ChangeTracker):
    collection: AsyncIOMotorCollection[dict[str, Any]]
    retort: Retort
    session: AsyncIOMotorClientSession
    _tracked_users: dict[str, User] = field(default_factory=dict, init=False)

    def track(self, user: User) -> None:
        """Отслеживать один объект"""
        if user._id is None:
            logger.warning("Cannot track user without _id")
            return

        self._tracked_users[user._id] = user
        logger.debug(f"Tracking user: {user._id}")

    def track_all(self, users: list[User]) -> None:
        """Отслеживать список объектов"""
        for user in users:
            self.track(user)

        logger.info(f"Tracking {len(users)} users")

    async def save(self) -> int:
        """
        Массово сохранить все отслеживаемые объекты через bulk_write.
        НЕ коммитит транзакцию - это делается автоматически provider'ом.
        """
        if not self._tracked_users:
            logger.info("No tracked users to save")
            return 0

        logger.info(f"Preparing bulk save for {len(self._tracked_users)} users")

        operations = []

        for user_id, user in self._tracked_users.items():
            try:
                object_id = ObjectId(user_id)
            except Exception:
                logger.warning(f"Invalid ObjectId: {user_id}, skipping")
                continue

            user_dict = self.retort.dump(user)
            user_dict.pop("_id", None)

            operations.append(
                ReplaceOne(
                    filter={"_id": object_id},
                    replacement=user_dict,
                    upsert=False,
                ),
            )

        if not operations:
            logger.warning("No valid operations to execute")
            self._tracked_users.clear()
            return 0

        # Передаем session в bulk_write для участия в транзакции
        result = await self.collection.bulk_write(
            operations,
            ordered=True,
            session=self.session,
        )

        logger.info(
            f"Bulk save completed: matched={result.matched_count}, "
            f"modified={result.modified_count}",
        )

        modified_count = result.modified_count
        self._tracked_users.clear()

        return modified_count

    async def commit(self) -> None:
        """
        Явно закоммитить транзакцию.
        """
        if self.session.in_transaction:
            await self.session.commit_transaction()
            logger.info("Transaction committed manually")
        else:
            logger.warning("No active transaction to commit")

    async def rollback(self) -> None:
        """
        Откатить транзакцию и очистить tracked объекты.
        """
        if self.session.in_transaction:
            await self.session.abort_transaction()
            logger.info("Transaction rolled back")
        else:
            logger.warning("No active transaction to rollback")

        self._tracked_users.clear()

    def clear(self) -> None:
        """Очистить отслеживаемые объекты без сохранения"""
        count = len(self._tracked_users)
        self._tracked_users.clear()
        logger.debug(f"Cleared {count} tracked users")
