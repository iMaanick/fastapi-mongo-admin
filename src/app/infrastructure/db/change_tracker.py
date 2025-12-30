import logging
from dataclasses import dataclass, field, is_dataclass
from typing import Any, TypeVar

from adaptix import Retort
from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorClientSession, AsyncIOMotorDatabase
from pymongo import ReplaceOne

from app.application.change_tracker import (
    ChangeTracker,
    ChangeTrackerError,
    CollectionMappingNotFoundError,
    EntityMissingIdError,
    EntityNotDataclassError,
    InvalidEntityIdError,
    NoValidOperationsError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class MongoChangeTracker(ChangeTracker):
    collection_mapping: dict[type, str]
    database: AsyncIOMotorDatabase[dict[str, Any]]
    retort: Retort
    session: AsyncIOMotorClientSession
    _tracked_entities: dict[type, dict[str, Any]] = field(
        default_factory=dict,
        init=False,
    )

    def track(self, entity: T) -> None:
        """Отслеживать один объект"""
        # Проверка что entity является dataclass
        if not is_dataclass(entity):
            raise EntityNotDataclassError(type(entity))

        entity_type = type(entity)

        # Проверка наличия _id
        if not hasattr(entity, "_id") or entity._id is None:  # noqa: SLF001
            raise EntityMissingIdError(entity_type)

        # Проверка маппинга коллекции
        if entity_type not in self.collection_mapping:
            raise CollectionMappingNotFoundError(entity_type)

        if entity_type not in self._tracked_entities:
            self._tracked_entities[entity_type] = {}

        self._tracked_entities[entity_type][entity._id] = entity  # noqa: SLF001
        logger.debug(
            "Tracking %s: %s",
            entity_type.__name__,
            entity._id,  # noqa: SLF001
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

        try:
            total_modified = await self._save_all_entity_types()
        except ChangeTrackerError:
            await self.rollback()
            raise
        except Exception:
            logger.exception("Unexpected error during save, rolling back")
            await self.rollback()
            raise
        else:
            self._tracked_entities.clear()
            return total_modified

    async def _save_all_entity_types(self) -> int:
        """Сохранить все типы entities и вернуть общее количество"""
        total_modified = 0

        for entity_type, entities_dict in self._tracked_entities.items():
            if not entities_dict:
                continue

            modified = await self._save_entity_type(entity_type, entities_dict)
            total_modified += modified

        return total_modified

    async def _save_entity_type(
            self,
            entity_type: type,
            entities_dict: dict[str, Any],
    ) -> int:
        """Сохранить один тип entities в MongoDB"""
        collection_name = self.collection_mapping.get(entity_type)
        if collection_name is None:
            raise CollectionMappingNotFoundError(entity_type)

        collection = self.database[collection_name]

        logger.info(
            "Preparing bulk save for %s %s -> %s",
            len(entities_dict),
            entity_type.__name__,
            collection_name,
        )

        operations = self._build_operations(entity_type, entities_dict)

        if not operations:
            raise NoValidOperationsError(entity_type)

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

        return result.modified_count

    def _build_operations(
            self,
            entity_type: type,
            entities_dict: dict[str, Any],
    ) -> list[ReplaceOne[dict[str, Any]]]:
        """Построить список операций ReplaceOne для bulk_write"""
        operations = []

        for entity_id, entity in entities_dict.items():
            try:
                object_id = ObjectId(entity_id)
            except InvalidId as e:
                raise InvalidEntityIdError(entity_id, entity_type) from e

            entity_dict = self.retort.dump(entity)
            entity_dict.pop("_id", None)

            operations.append(
                ReplaceOne(
                    filter={"_id": object_id},
                    replacement=entity_dict,
                    upsert=False,
                ),
            )

        return operations

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
        total_count = sum(map(len, self._tracked_entities.values()))
        self._tracked_entities.clear()
        logger.debug("Cleared %s tracked entities", total_count)
