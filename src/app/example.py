import copy
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, SupportsIndex
from weakref import ref

from bson import ObjectId
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)


class InstanceState:
    """Состояние отдельного экземпляра для отслеживания изменений"""

    def __init__(self, instance: Any) -> None:
        self.instance_ref = ref(instance)
        self.changed_fields: set[str] = set()
        self.original_values: dict[str, Any] = {}

    def mark_changed(self, field_name: str, original_value: Any) -> None:
        if field_name not in self.changed_fields:
            if isinstance(original_value, list | dict):
                self.original_values[field_name] = copy.deepcopy(
                    original_value,
                )
            else:
                self.original_values[field_name] = original_value
        self.changed_fields.add(field_name)

    def get_changed_fields(self) -> set[str]:
        return self.changed_fields.copy()

    def get_original_value(self, field_name: str) -> Any:
        return self.original_values.get(field_name)


class TrackedList(list[Any]):
    """Список с отслеживанием изменений"""

    def __init__(
        self,
        data: list[Any],
        instance: Any,
        field_name: str,
        session_instance_states: dict[int, InstanceState],
    ) -> None:
        super().__init__(data)
        self._instance_id = id(instance)
        self._field_name = field_name
        self._session_instance_states = session_instance_states
        self._mark_initial_state()

    def _mark_initial_state(self) -> None:
        if self._instance_id in self._session_instance_states:
            state = self._session_instance_states[self._instance_id]
            if self._field_name not in state.original_values:
                state.original_values[self._field_name] = copy.deepcopy(
                    list(self),
                )

    def _mark_changed(self) -> None:
        if self._instance_id in self._session_instance_states:
            state = self._session_instance_states[self._instance_id]
            state.changed_fields.add(self._field_name)

    def append(self, item: Any) -> None:
        self._mark_changed()
        super().append(item)

    def extend(self, items: Any) -> None:
        self._mark_changed()
        super().extend(items)

    def insert(self, index: SupportsIndex, item: Any) -> None:
        self._mark_changed()
        super().insert(index, item)

    def remove(self, item: Any) -> None:
        self._mark_changed()
        super().remove(item)

    def pop(self, index: SupportsIndex = -1) -> Any:
        self._mark_changed()
        return super().pop(index)

    def clear(self) -> None:
        self._mark_changed()
        super().clear()

    def __setitem__(self, key: Any, value: Any) -> None:
        self._mark_changed()
        super().__setitem__(key, value)

    def __delitem__(self, key: Any) -> None:
        self._mark_changed()
        super().__delitem__(key)

    def __getitem__(self, key: Any) -> Any:
        item = super().__getitem__(key)
        if hasattr(item, "__dict__") and not isinstance(
            item,
            TrackedObject,
        ):
            tracked_item = TrackedObject(item, self)
            super().__setitem__(key, tracked_item)
            return tracked_item
        return item


class TrackedObject:
    """Прокси для отслеживания изменений вложенных объектов"""

    def __init__(self, obj: Any, parent: Any) -> None:
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "_parent", parent)

    def __getattr__(self, name: str) -> Any:
        obj = object.__getattribute__(self, "_obj")
        value = getattr(obj, name)

        if hasattr(value, "__dict__") and not isinstance(
            value,
            TrackedObject,
        ):
            return TrackedObject(value, self)

        return value

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return

        self._mark_parent_changed()
        obj = object.__getattribute__(self, "_obj")
        setattr(obj, name, value)

    def _mark_parent_changed(self) -> None:
        parent = object.__getattribute__(self, "_parent")

        if isinstance(parent, TrackedList):
            parent._mark_changed()  # noqa: SLF001
        elif isinstance(parent, TrackedObject):
            parent._mark_parent_changed()  # noqa: SLF001

    def __repr__(self) -> str:
        obj = object.__getattribute__(self, "_obj")
        return repr(obj)


# ============= Исключения =============


class DatabaseNotSetError(ValueError):
    """Ошибка при отсутствии подключения к БД"""

    def __init__(self) -> None:
        super().__init__("Database not set.")


class CollectionNotMappedError(ValueError):
    """Ошибка когда класс не имеет маппинга коллекции"""

    def __init__(self, cls: type) -> None:
        super().__init__(
            f"Collection name not found for {cls.__name__}. "
            f"Please add it to collection_mapping.",
        )


class ClassAlreadyInstrumentedError(RuntimeError):
    """Ошибка когда класс уже инструментирован"""

    def __init__(self, cls: type) -> None:
        super().__init__(
            f"Class {cls.__name__} is already instrumented for tracking. "
            f"Cannot instrument the same class twice.",
        )


class ClassNotInstrumentedError(RuntimeError):
    """Ошибка когда класс не инструментирован"""

    def __init__(self, cls: type) -> None:
        super().__init__(
            f"Class {cls.__name__} is not instrumented for tracking. "
            f"Call instrument_class() before using it with Session.",
        )


# ============= Инструментация классов =============


# Маркер для проверки инструментированности класса
_INSTRUMENTED_MARKER = "__session_instrumented__"


def is_class_instrumented(cls: type) -> bool:
    """Проверяет, инструментирован ли класс для отслеживания"""
    return hasattr(cls, _INSTRUMENTED_MARKER)


def instrument_class(cls: type) -> None:
    """
    Инструментирует класс для отслеживания изменений.

    Raises:
        ClassAlreadyInstrumentedError: Если класс уже инструментирован
    """
    if is_class_instrumented(cls):
        raise ClassAlreadyInstrumentedError(cls)

    # Сохраняем оригинальный __setattr__
    original_setattr = cls.__setattr__

    def tracking_setattr(instance: Any, name: str, value: Any) -> None:
        instance_id = id(instance)
        session_ref = getattr(instance, "_session_ref", None)

        if session_ref is not None:
            session = session_ref()
            if session is not None:
                session_states = session._instance_states  # noqa: SLF001
                if instance_id in session_states and not name.startswith("_"):
                    state = session_states[instance_id]

                    if hasattr(instance, name):
                        old_value = getattr(instance, name)
                        state.mark_changed(name, old_value)

                    if isinstance(value, list) and not isinstance(
                        value,
                        TrackedList,
                    ):
                        value = TrackedList(
                            value,
                            instance,
                            name,
                            session_states,
                        )

        original_setattr(instance, name, value)  # type: ignore[call-arg]

    def get_changed_fields(self: Any) -> set[str]:
        instance_id = id(self)
        session_ref = getattr(self, "_session_ref", None)

        if session_ref is not None:
            session = session_ref()
            if (
                session is not None
                and instance_id in session._instance_states  # noqa: SLF001
            ):
                states: dict[int, InstanceState] = (
                    session._instance_states  # noqa: SLF001
                )
                return states[instance_id].get_changed_fields()
        return set()

    def get_original_value(self: Any, field_name: str) -> Any:
        instance_id = id(self)
        session_ref = getattr(self, "_session_ref", None)

        if session_ref is not None:
            session = session_ref()
            if (
                session is not None
                and instance_id in session._instance_states  # noqa: SLF001
            ):
                states: dict[int, InstanceState] = (
                    session._instance_states  # noqa: SLF001
                )
                return states[instance_id].get_original_value(field_name)
        return None

    # Патчим класс
    cls.__setattr__ = tracking_setattr  # type: ignore[assignment]
    cls.get_changed_fields = get_changed_fields  # type: ignore[attr-defined]
    cls.get_original_value = get_original_value  # type: ignore[attr-defined]

    # Помечаем класс как инструментированный
    setattr(cls, _INSTRUMENTED_MARKER, True)


class Session:
    """
    Session в стиле SQLAlchemy с отслеживанием изменений и MongoDB

    Каждая сессия создается для одного запроса и имеет собственное состояние.
    """

    def __init__(
        self,
        db: AsyncIOMotorDatabase[dict[str, Any]],
        mongo_session: AsyncIOMotorClientSession | None = None,
        collection_mapping: dict[type, str] | None = None,
    ) -> None:
        """
        Инициализация сессии

        Args:
            db: AsyncIOMotorDatabase
            mongo_session: MongoDB session для транзакций (опционально)
            collection_mapping: Маппинг класс -> имя коллекции
        """
        self.db = db
        self.mongo_session = mongo_session
        self.collection_mapping = collection_mapping or {}
        self._instance_states: dict[int, InstanceState] = {}
        self._tracked_instances: list[Any] = []

    def add(self, instance: Any) -> Any:
        """
        Добавляет экземпляр под отслеживание

        Raises:
            ClassNotInstrumentedError: Если класс не инструментирован
        """
        cls = instance.__class__

        if not is_class_instrumented(cls):
            raise ClassNotInstrumentedError(cls)

        instance_id = id(instance)

        if instance_id not in self._instance_states:
            self._instance_states[instance_id] = InstanceState(instance)

        self._tracked_instances.append(instance)
        self._wrap_mutable_fields(instance)

        return instance

    def _wrap_mutable_fields(self, instance: Any) -> None:
        """Оборачивает mutable поля в отслеживаемые прокси"""
        object.__setattr__(instance, "_session_ref", ref(self))

        for field_name in dir(instance):
            if field_name.startswith("_"):
                continue
            try:
                value = getattr(instance, field_name)
                if isinstance(value, list) and not isinstance(
                    value,
                    TrackedList,
                ):
                    tracked_list = TrackedList(
                        value,
                        instance,
                        field_name,
                        self._instance_states,
                    )
                    object.__setattr__(instance, field_name, tracked_list)
            except (AttributeError, TypeError):
                # Игнорируем ошибки при обходе полей (property, slots)
                pass

    def _get_collection_name(self, instance: Any) -> str:
        """Получает имя коллекции для экземпляра"""
        cls = instance.__class__
        collection_name = self.collection_mapping.get(cls)

        if collection_name is None:
            raise CollectionNotMappedError(cls)

        return collection_name

    def _serialize_value(self, value: Any) -> Any:
        """Сериализует значение для MongoDB"""
        if isinstance(value, TrackedObject):
            value = object.__getattribute__(value, "_obj")

        if isinstance(value, TrackedList):
            return [self._serialize_value(item) for item in value]

        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]

        if isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}

        if is_dataclass(value) and not isinstance(value, type):
            result = {}
            for f in fields(value):
                field_value = getattr(value, f.name)
                result[f.name] = self._serialize_value(field_value)
            return result

        return value

    def build_update_query(self, instance: Any) -> dict[str, Any] | None:
        """Создает MongoDB update запрос на основе изменений"""
        instance_id = id(instance)
        if instance_id not in self._instance_states:
            return None

        state = self._instance_states[instance_id]
        changed_fields = state.get_changed_fields()

        if not changed_fields:
            return None

        set_operations = {
            field_name: self._serialize_value(
                getattr(instance, field_name),
            )
            for field_name in changed_fields
            if field_name != "_id"
        }

        return {"$set": set_operations} if set_operations else None

    async def commit(self) -> None:
        """Сохраняет все изменения в MongoDB"""
        if self.db is None:
            raise DatabaseNotSetError

        for instance in self._tracked_instances:
            await self._commit_instance(instance)

    async def _commit_instance(self, instance: Any) -> None:
        """Сохраняет изменения одного экземпляра"""
        instance_id = id(instance)

        if instance_id not in self._instance_states:
            return

        state = self._instance_states[instance_id]
        if not state.changed_fields:
            return

        collection_name = self._get_collection_name(instance)
        collection: AsyncIOMotorCollection[dict[str, Any]] = self.db[collection_name]

        update_query = self.build_update_query(instance)
        if not update_query:
            return

        doc_id = getattr(instance, "_id", None)
        operation_kwargs: dict[str, Any] = {}
        if self.mongo_session:
            operation_kwargs["session"] = self.mongo_session

        if doc_id:
            await self._update_document(
                collection,
                doc_id,
                update_query,
                operation_kwargs,
            )
        else:
            await self._insert_document(
                collection,
                instance,
                operation_kwargs,
            )

        state.changed_fields.clear()
        state.original_values.clear()

    @staticmethod
    async def _update_document(
        collection: AsyncIOMotorCollection[dict[str, Any]],
        doc_id: str | ObjectId,
        update_query: dict[str, Any],
        operation_kwargs: dict[str, Any],
    ) -> None:
        """Обновляет существующий документ"""
        filter_query = {
            "_id": ObjectId(doc_id) if isinstance(doc_id, str) else doc_id,
        }
        await collection.update_one(
            filter_query,
            update_query,
            **operation_kwargs,
        )

    async def _insert_document(
        self,
        collection: AsyncIOMotorCollection[dict[str, Any]],
        instance: Any,
        operation_kwargs: dict[str, Any],
    ) -> None:
        """Вставляет новый документ"""
        doc_data = self._serialize_value(instance)
        result = await collection.insert_one(doc_data, **operation_kwargs)
        # Используем object.__setattr__ чтобы обойти tracking_setattr
        object.__setattr__(instance, "_id", result.inserted_id)

    async def flush(self) -> None:
        """Синоним для commit"""
        await self.commit()

    def rollback(self) -> None:
        """Откатывает изменения"""
        for instance in self._tracked_instances:
            self._rollback_instance(instance)

    def _rollback_instance(self, instance: Any) -> None:
        """Откатывает изменения одного экземпляра"""
        instance_id = id(instance)
        if instance_id not in self._instance_states:
            return

        state = self._instance_states[instance_id]
        for field_name, value in list(state.original_values.items()):
            if isinstance(value, list):
                restored = copy.deepcopy(value)
                tracked = TrackedList(
                    restored,
                    instance,
                    field_name,
                    self._instance_states,
                )
                setattr(instance, field_name, tracked)
            else:
                setattr(instance, field_name, value)

        state.changed_fields.clear()
        state.original_values.clear()

    def close(self) -> None:
        """Очистка ресурсов сессии"""
        for instance in self._tracked_instances:
            instance_id = id(instance)
            if instance_id in self._instance_states:
                del self._instance_states[instance_id]
        self._tracked_instances.clear()


# ============= Примеры использования =============


@dataclass
class Tag:
    title: str

    def __repr__(self) -> str:
        return f"Tag({self.title})"


@dataclass
class User:
    username: str
    email: str
    tags: list[Tag] = field(default_factory=list)
    _id: str | None = None


@dataclass
class Article:
    title: str
    content: str
    _id: str | None = None


# Инструментируем классы перед использованием
instrument_class(Tag)
instrument_class(User)
instrument_class(Article)


async def example_with_transaction() -> None:
    """Пример использования с транзакцией MongoDB"""
    client: AsyncIOMotorClient[dict[str, Any]] = AsyncIOMotorClient(
        "mongodb://localhost:27017",
    )
    db: AsyncIOMotorDatabase[dict[str, Any]] = client.mydb

    async with (
        await client.start_session() as mongo_session,
        mongo_session.start_transaction(),
    ):
        session = Session(
            db=db,
            mongo_session=mongo_session,
            collection_mapping={
                User: "users",
                Article: "blog_articles",
            },
        )

        try:
            user = User(
                username="john",
                email="john@example.com",
                tags=[Tag("python")],
                _id="507f1f77bcf86cd799439011",
            )

            session.add(user)
            user.username = "john_doe"
            user.tags.append(Tag("mongodb"))

            await session.commit()
        finally:
            session.close()

    client.close()


async def example_simple_request() -> None:
    """Пример для одного API запроса без транзакции"""
    client: AsyncIOMotorClient[dict[str, Any]] = AsyncIOMotorClient(
        "mongodb://localhost:27017",
    )
    db: AsyncIOMotorDatabase[dict[str, Any]] = client.mydb

    session = Session(
        db=db,
        collection_mapping={User: "users"},
    )

    try:
        user = User(username="alice", email="alice@example.com")

        session.add(user)
        user.tags.append(Tag("python"))
        user.tags[0].title = "python3"

        await session.commit()
    finally:
        session.close()

    client.close()


async def example_uninstrumented_class() -> None:
    """Пример ошибки при использовании неинструментированного класса"""

    @dataclass
    class UninstrumentedUser:
        username: str

    client: AsyncIOMotorClient[dict[str, Any]] = AsyncIOMotorClient(
        "mongodb://localhost:27017",
    )
    db: AsyncIOMotorDatabase[dict[str, Any]] = client.mydb

    session = Session(
        db=db,
        collection_mapping={UninstrumentedUser: "users"},
    )

    try:
        user = UninstrumentedUser(username="test")
        session.add(user)  # Вызовет ClassNotInstrumentedError
    except ClassNotInstrumentedError as e:
        print(f"Error: {e}")
    finally:
        session.close()
        client.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(example_simple_request())
    asyncio.run(example_with_transaction())
    asyncio.run(example_uninstrumented_class())
