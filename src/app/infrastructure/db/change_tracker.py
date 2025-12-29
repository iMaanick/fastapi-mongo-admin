import logging
from dataclasses import dataclass, field, is_dataclass
from typing import Any, TypeVar

from adaptix import Retort
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClientSession, AsyncIOMotorDatabase
from pymongo import ReplaceOne

from app.application.change_tracker import ChangeTracker

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class MongoChangeTracker(ChangeTracker):
    collection_mapping: dict[type, str]  # {User: "users", Course: "courses"}
    database: AsyncIOMotorDatabase[dict[str, Any]]
    retort: Retort
    session: AsyncIOMotorClientSession
    _tracked_entities: dict[type, dict[str, Any]] = field(
        default_factory=dict,
        init=False,
    )

    def track(self, entity: T) -> None:
        """Отслеживать один объект"""
        if not is_dataclass(entity):
            raise TypeError(
                f"{type(entity).__name__} must be a dataclass",
            )

        if not hasattr(entity, "_id") or entity._id is None:
            logger.warning(
                "Cannot track %s without _id",
                type(entity).__name__,
            )
            return

        entity_type = type(entity)

        if entity_type not in self.collection_mapping:
            logger.warning(
                "No collection mapping for %s",
                entity_type.__name__,
            )
            return

        if entity_type not in self._tracked_entities:
            self._tracked_entities[entity_type] = {}

        self._tracked_entities[entity_type][entity._id] = entity
        logger.debug(
            "Tracking %s: %s",
            entity_type.__name__,
            entity._id,
        )

    def track_all(self, entities: list[T]) -> None:
        """Отслеживать список объектов"""
        for entity in entities:
            self.track(entity)

        logger.info("Tracking %s entities", len(entities))

    async def save(self) -> int:
        """
        Массово сохранить все отслеживаемые объекты через bulk_write.
        НЕ коммитит транзакцию, это делается commit
        """
        if not self._tracked_entities:
            logger.info("No tracked entities to save")
            return 0

        total_modified = 0

        for entity_type, entities_dict in self._tracked_entities.items():
            if not entities_dict:
                continue

            collection_name = self.collection_mapping.get(entity_type)
            if collection_name is None:
                logger.warning(
                    "No collection mapped for %s, skipping",
                    entity_type.__name__,
                )
                continue

            collection = self.database[collection_name]

            logger.info(
                "Preparing bulk save for %s %s -> %s",
                len(entities_dict),
                entity_type.__name__,
                collection_name,
            )

            operations = []

            for entity_id, entity in entities_dict.items():
                try:
                    object_id = ObjectId(entity_id)
                except Exception:
                    logger.warning("Invalid ObjectId: %s, skipping", entity_id)
                    continue

                entity_dict = self.retort.dump(entity)
                entity_dict.pop("_id", None)

                operations.append(
                    ReplaceOne(
                        filter={"_id": object_id},
                        replacement=entity_dict,
                        upsert=False,
                    ),
                )

            if not operations:
                logger.warning(
                    "No valid operations for %s",
                    entity_type.__name__,
                )
                continue

            result = await collection.bulk_write(
                operations,
                ordered=True,
                session=self.session,
            )

            logger.info(
                "Bulk save completed for %s: matched=%s, modified=%s",
                entity_type.__name__,
                result.matched_count,
                result.modified_count,
            )

            total_modified += result.modified_count

        self._tracked_entities.clear()
        return total_modified

    async def commit(self) -> None:
        """Явно закоммитить транзакцию."""
        if self.session.in_transaction:  # type: ignore[truthy-function]
            await self.session.commit_transaction()
            logger.info("Transaction committed manually")
        else:
            logger.warning("No active transaction to commit")

    async def rollback(self) -> None:
        """Откатить транзакцию и очистить tracked объекты."""
        if self.session.in_transaction:  # type: ignore[truthy-function]
            await self.session.abort_transaction()
            logger.info("Transaction rolled back")
        else:
            logger.warning("No active transaction to rollback")

        self._tracked_entities.clear()

    def clear(self) -> None:
        """Очистить отслеживаемые объекты без сохранения"""
        total_count = sum(len(entities) for entities in self._tracked_entities.values())
        self._tracked_entities.clear()
        logger.debug("Cleared %s tracked entities", total_count)
