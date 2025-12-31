import logging
from contextlib import asynccontextmanager
from pprint import pprint
from typing import Any, Generic, TypeVar

from adaptix import Retort
from adaptix.load_error import AggregateLoadError, ValidationLoadError
from adaptix.struct_trail import get_trail
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.requests import Request
from starlette_admin import BaseModelView
from starlette_admin.exceptions import FormValidationError

from app.application.exceptions.base import EntityNotFoundError
from app.infrastructure.db.generic_mongo_repository import (
    GenericMongoRepository,
)
from app.infrastructure.db.query_builder import build_mongo_filter

logger = logging.getLogger(__name__)

T = TypeVar("T")


class GenericMongoView(BaseModelView, Generic[T]):
    """Generic админка для MongoDB моделей"""

    model_type: type[T]
    collection_name: str
    database_name: str

    def __init__(
            self,
            client: AsyncIOMotorClient[dict[str, Any]],
            retort: Retort,
    ):
        super().__init__()
        self.client = client
        self.retort = retort
        self._repository = GenericMongoRepository(
            client=client,
            database_name=self.database_name,
            collection_name=self.collection_name,
            model_type=self.model_type,
            retort=retort,
        )

    @asynccontextmanager
    async def _transaction(self):
        """Context manager для транзакций"""
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    yield session
                    await session.commit_transaction()
                    logger.debug("Transaction committed")
                except Exception:
                    logger.exception("Transaction failed, rolling back")
                    if session.in_transaction:
                        await session.abort_transaction()
                    raise

    def _normalize_form_data(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Нормализовать данные формы для adaptix.
        """
        normalized = {}

        for key, value in data.items():
            if isinstance(value, dict) and value:
                all_keys_numeric = all(k.isdigit() for k in value.keys())

                if all_keys_numeric:
                    try:
                        int_keys = {int(k): k for k in value.keys()}
                        sorted_indices = sorted(int_keys.keys())

                        if sorted_indices == list(range(len(sorted_indices))):
                            normalized[key] = [
                                value[int_keys[i]] for i in sorted_indices
                            ]
                            logger.debug(
                                "Converted '%s' dict→list (%d items)",
                                key,
                                len(sorted_indices),
                            )
                        else:
                            normalized[key] = value
                    except (ValueError, TypeError):
                        normalized[key] = value
                elif all(isinstance(k, str) for k in value.keys()):
                    normalized[key] = [value]
                    logger.debug(
                        "Wrapped single object '%s' into list",
                        key,
                    )
                else:
                    normalized[key] = value
            else:
                normalized[key] = value

        return normalized

    def _convert_adaptix_errors_to_form_errors(
            self, exc: AggregateLoadError,
    ) -> dict[str, str]:
        """
        Преобразовать ошибки adaptix в формат starlette-admin.

        Returns:
            dict[field_name, error_message]
        """
        errors = {}

        for error in exc.exceptions:
            # Получаем путь к полю, где произошла ошибка
            trail = list(get_trail(error))

            # Формируем имя поля для формы
            if trail:
                # Преобразуем ['lessons', 0, 'title'] → 'lessons.0.title'
                field_path = ".".join(str(part) for part in trail)
            else:
                field_path = "__all__"  # Общая ошибка модели

            # Формируем сообщение об ошибке
            if isinstance(error, ValidationLoadError):
                error_msg = error.msg
            else:
                error_msg = str(error)

            errors[field_path] = error_msg

        return errors

    async def find_all(
            self,
            request: Request,
            skip: int = 0,
            limit: int = 100,
            where: dict[str, Any] | str | None = None,
            order_by: list[str] | None = None,
    ) -> list[Any]:
        """Получение списка сущностей с фильтрацией и сортировкой"""
        mongo_filter = build_mongo_filter(where)
        sort = self._build_sort(order_by)

        entities = await self._repository.get_all(
            filter_query=mongo_filter,
            skip=skip,
            limit=max(0, limit),
            sort=sort,
            session=None,
        )

        return entities

    async def count(
            self,
            request: Request,
            where: dict[str, Any] | str | None = None,
    ) -> int:
        """Подсчет количества документов с учётом фильтра"""
        mongo_filter = build_mongo_filter(where)

        entities = await self._repository.get_all(
            filter_query=mongo_filter,
            skip=0,
            limit=0,
            sort=None,
            session=None,
        )

        return len(entities)

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        """Получение сущности по ID"""
        async with await self.client.start_session() as session:
            entity = await self._repository.get_by_id(
                str(pk),
                session=session,
            )
            return entity

    async def find_by_pks(
            self,
            request: Request,
            pks: list[Any],
    ) -> list[Any]:
        """Получение сущностей по списку ID"""
        if not pks:
            return []

        object_ids = [ObjectId(str(pk)) for pk in pks]

        async with await self.client.start_session() as session:
            entities = await self._repository.get_all(
                filter_query={"_id": {"$in": object_ids}},
                skip=0,
                limit=0,
                sort=None,
                session=session,
            )
            return entities

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        """Создание сущности"""
        async with self._transaction() as session:
            normalized_data = self._normalize_form_data(data)

            try:
                entity = self.retort.load(normalized_data, self.model_type)
            except AggregateLoadError as e:
                # Преобразуем ошибки adaptix в формат starlette-admin
                errors = self._convert_adaptix_errors_to_form_errors(e)
                raise FormValidationError(errors)

            await self._repository.add(entity, session)
            logger.info("✅ Created %s successfully", self.model_type.__name__)
            return entity

    async def edit(
            self,
            request: Request,
            pk: Any,
            data: dict[str, Any],
    ) -> Any:
        """Редактирование сущности"""
        pprint(data)
        async with self._transaction() as session:
            existing = await self._repository.get_by_id(str(pk), session)
            if existing is None:
                raise EntityNotFoundError(
                    entity_type=self.model_type,
                    field_name="_id",
                    field_value=pk,
                )

            normalized_data = self._normalize_form_data(data)
            normalized_data["_id"] = str(pk)

            try:
                updated_entity = self.retort.load(normalized_data, self.model_type)
            except AggregateLoadError as e:
                errors = self._convert_adaptix_errors_to_form_errors(e)
                raise FormValidationError(errors)

            await self._repository.update(str(pk), updated_entity, session)
            logger.info("✅ Updated %s: %s", self.model_type.__name__, pk)
            return updated_entity

    async def delete(self, request: Request, pks: list[Any]) -> int | None:
        """Удаление сущностей"""
        async with self._transaction() as session:
            deleted = 0
            for pk in pks:
                success = await self._repository.delete(str(pk), session)
                if success:
                    deleted += 1

            logger.info(
                "✅ Deleted %s %s out of %s",
                deleted,
                self.model_type.__name__,
                len(pks),
            )
            return deleted

    @staticmethod
    def _build_sort(
            order_by: list[str] | None,
    ) -> list[tuple[str, int]] | None:
        """Построить sort для MongoDB из order_by"""
        if not order_by:
            return None

        sort = []
        for item in order_by:
            key, direction = item.split(maxsplit=1)
            sort_direction = -1 if direction.lower() == "desc" else 1
            sort.append((key, sort_direction))

        return sort
