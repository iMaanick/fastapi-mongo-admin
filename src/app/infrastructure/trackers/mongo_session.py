import logging
from dataclasses import dataclass, field, is_dataclass
from typing import Any, TypeVar

from adaptix import Retort
from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorClientSession, AsyncIOMotorDatabase

from app.application.change_tracker import (
    ChangeTrackerError,
    CollectionMappingNotFoundError,
    EntityNotDataclassError,
    InvalidEntityIdError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class MongoSession:
    """Session for MongoDB with change tracking and partial updates"""

    collection_mapping: dict[type, str]
    database: AsyncIOMotorDatabase[dict[str, Any]]
    retort: Retort
    session: AsyncIOMotorClientSession

    # Track original snapshots and current entities
    _tracked_entities: dict[type, dict[str, Any]] = field(
        default_factory=dict,
        init=False,
    )
    _original_snapshots: dict[type, dict[str, dict[str, Any]]] = field(
        default_factory=dict,
        init=False,
    )
    # Track entities pending insertion (where _id is None)
    _pending_inserts: dict[type, list[Any]] = field(
        default_factory=dict,
        init=False,
    )

    def add(self, entity: T) -> None:
        """Track a single entity for change detection"""
        if not is_dataclass(entity):
            raise EntityNotDataclassError(type(entity))

        entity_type = type(entity)

        if entity_type not in self.collection_mapping:
            raise CollectionMappingNotFoundError(entity_type)

        # Handle entities without _id (pending insert)
        if not hasattr(entity, "_id") or entity._id is None:  # noqa: SLF001
            if entity_type not in self._pending_inserts:
                self._pending_inserts[entity_type] = []
            # Avoid duplicates
            if entity not in self._pending_inserts[entity_type]:
                self._pending_inserts[entity_type].append(entity)
                logger.debug(
                    "Tracking new entity for insert: %s",
                    entity_type.__name__,
                )
            return

        # Initialize tracking structures for this entity type
        if entity_type not in self._tracked_entities:
            self._tracked_entities[entity_type] = {}
            self._original_snapshots[entity_type] = {}

        entity_id = str(entity._id)  # noqa: SLF001

        # Store current entity reference
        self._tracked_entities[entity_type][entity_id] = entity

        # Store original snapshot only if not already tracked
        if entity_id not in self._original_snapshots[entity_type]:
            original_dump = self.retort.dump(entity)
            original_dump.pop("_id", None)
            self._original_snapshots[entity_type][entity_id] = original_dump
            logger.debug(
                "Tracking %s: %s (snapshot created)",
                entity_type.__name__,
                entity_id,
            )

    def add_all(self, entities: list[T]) -> None:
        """Track multiple entities"""
        for entity in entities:
            self.add(entity)
        logger.info("Tracking %s entities", len(entities))

    async def flush(self) -> None:
        """Execute pending operations (inserts and updates) without committing transaction"""
        if not self._tracked_entities and not self._pending_inserts:
            logger.info("No tracked entities to flush")
            return

        try:
            await self._process_inserts()
            await self._process_updates()

            # Update snapshots after successful flush
            self._update_snapshots()

        except ChangeTrackerError:
            raise
        except Exception:
            logger.exception("Unexpected error during flush")
            raise

    def _update_snapshots(self) -> None:
        """Update snapshots to current state after successful flush"""
        for entity_type, entities_dict in self._tracked_entities.items():
            for entity_id, entity in entities_dict.items():
                current_dump = self.retort.dump(entity)
                current_dump.pop("_id", None)
                self._original_snapshots[entity_type][entity_id] = current_dump

    async def _process_inserts(self) -> None:
        """Process all pending insert operations"""
        for entity_type, entities in self._pending_inserts.items():
            if not entities:
                continue

            collection_name = self.collection_mapping.get(entity_type)
            if collection_name is None:
                raise CollectionMappingNotFoundError(entity_type)

            collection = self.database[collection_name]

            logger.info(
                "Inserting %s new %s entities",
                len(entities),
                entity_type.__name__,
            )

            for entity in entities:
                entity_dict = self.retort.dump(entity)
                entity_dict.pop("_id", None)  # Let MongoDB generate _id

                result = await collection.insert_one(
                    entity_dict,
                    session=self.session,
                )

                # Update entity with generated _id
                entity._id = result.inserted_id  # noqa: SLF001

                logger.debug(
                    "Inserted %s with _id: %s",
                    entity_type.__name__,
                    result.inserted_id,
                )

        # Clear pending inserts after successful insertion
        self._pending_inserts.clear()

    async def _process_updates(self) -> None:
        """Process all pending update operations using partial updates"""
        for entity_type, entities_dict in self._tracked_entities.items():
            if not entities_dict:
                continue

            await self._update_entity_type(entity_type, entities_dict)

    async def _update_entity_type(
        self,
        entity_type: type,
        entities_dict: dict[str, Any],
    ) -> None:
        """Update entities of a specific type with change detection"""
        collection_name = self.collection_mapping.get(entity_type)
        if collection_name is None:
            raise CollectionMappingNotFoundError(entity_type)

        collection = self.database[collection_name]

        for entity_id, entity in entities_dict.items():
            try:
                object_id = ObjectId(entity_id)
            except InvalidId as e:
                raise InvalidEntityIdError(entity_id, entity_type) from e

            # Get current and original state
            current_dump = self.retort.dump(entity)
            current_dump.pop("_id", None)

            original_dump = self._original_snapshots[entity_type].get(entity_id)

            if original_dump is None:
                logger.warning(
                    "No original snapshot for %s:%s, skipping",
                    entity_type.__name__,
                    entity_id,
                )
                continue

            # Compare top-level keys only
            update_fields = self._get_changed_fields(original_dump, current_dump)

            if not update_fields:
                logger.debug(
                    "No changes detected for %s:%s",
                    entity_type.__name__,
                    entity_id,
                )
                continue

            # Execute update with $set operator
            result = await collection.update_one(
                {"_id": object_id},
                {"$set": update_fields},
                session=self.session,
            )

            if result.modified_count > 0:
                logger.debug(
                    "Updated %s:%s with fields: %s",
                    entity_type.__name__,
                    entity_id,
                    list(update_fields.keys()),
                )

    def _get_changed_fields(
        self,
        original: dict[str, Any],
        current: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Compare top-level keys of two dumps and return changed fields.
        This works correctly because MongoDB can update entire nested structures.
        """
        update_fields = {}

        # Get all keys from both dicts
        all_keys = set(original.keys()) | set(current.keys())

        for key in all_keys:
            original_value = original.get(key)
            current_value = current.get(key)

            # If value changed or key was added
            if original_value != current_value:
                update_fields[key] = current_value

        return update_fields

    async def commit(self) -> None:
        """Flush changes and commit transaction"""
        await self.flush()

        if self.session.in_transaction:  # type: ignore[truthy-function]
            await self.session.commit_transaction()
            logger.info("Transaction committed")
        else:
            logger.warning("No active transaction to commit")

        # Clear tracking after successful commit
        self._tracked_entities.clear()
        self._original_snapshots.clear()

    async def rollback(self) -> None:
        """Rollback transaction and clear all tracked entities"""
        if self.session.in_transaction:  # type: ignore[truthy-function]
            await self.session.abort_transaction()
            logger.info("Transaction rolled back")
        else:
            logger.warning("No active transaction to rollback")

        self._tracked_entities.clear()
        self._original_snapshots.clear()
        self._pending_inserts.clear()
