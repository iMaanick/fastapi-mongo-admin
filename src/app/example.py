import copy
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, Set, Dict, Optional, Type
from weakref import ref
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorClientSession,
)
from bson import ObjectId


class InstanceState:
    """Состояние отдельного экземпляра для отслеживания изменений"""

    def __init__(self, instance: Any):
        self.instance_ref = ref(instance)
        self.changed_fields: Set[str] = set()
        self.original_values: Dict[str, Any] = {}

    def mark_changed(self, field_name: str, original_value: Any):
        if field_name not in self.changed_fields:
            if isinstance(original_value, (list, dict)):
                self.original_values[field_name] = copy.deepcopy(original_value)
            else:
                self.original_values[field_name] = original_value
        self.changed_fields.add(field_name)

    def get_changed_fields(self) -> Set[str]:
        return self.changed_fields.copy()

    def get_original_value(self, field_name: str) -> Any:
        return self.original_values.get(field_name)


class TrackedList(list):
    """Список с отслеживанием изменений"""

    def __init__(self, data, instance, field_name, session_instance_states):
        super().__init__(data)
        self._instance_id = id(instance)
        self._field_name = field_name
        self._session_instance_states = session_instance_states
        self._mark_initial_state()

    def _mark_initial_state(self):
        if self._instance_id in self._session_instance_states:
            state = self._session_instance_states[self._instance_id]
            if self._field_name not in state.original_values:
                state.original_values[self._field_name] = copy.deepcopy(list(self))

    def _mark_changed(self):
        if self._instance_id in self._session_instance_states:
            state = self._session_instance_states[self._instance_id]
            state.changed_fields.add(self._field_name)

    def append(self, item):
        self._mark_changed()
        super().append(item)

    def extend(self, items):
        self._mark_changed()
        super().extend(items)

    def insert(self, index, item):
        self._mark_changed()
        super().insert(index, item)

    def remove(self, item):
        self._mark_changed()
        super().remove(item)

    def pop(self, index=-1):
        self._mark_changed()
        return super().pop(index)

    def clear(self):
        self._mark_changed()
        super().clear()

    def __setitem__(self, key, value):
        self._mark_changed()
        super().__setitem__(key, value)

    def __delitem__(self, key):
        self._mark_changed()
        super().__delitem__(key)

    def __getitem__(self, key):
        item = super().__getitem__(key)
        if hasattr(item, "__dict__") and not isinstance(item, TrackedObject):
            tracked_item = TrackedObject(item, self)
            super().__setitem__(key, tracked_item)
            return tracked_item
        return item


class TrackedObject:
    """Прокси для отслеживания изменений вложенных объектов"""

    def __init__(self, obj: Any, parent):
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "_parent", parent)

    def __getattr__(self, name: str) -> Any:
        obj = object.__getattribute__(self, "_obj")
        value = getattr(obj, name)

        if hasattr(value, "__dict__") and not isinstance(value, TrackedObject):
            return TrackedObject(value, self)

        return value

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return

        self._mark_parent_changed()
        obj = object.__getattribute__(self, "_obj")
        setattr(obj, name, value)

    def _mark_parent_changed(self):
        parent = object.__getattribute__(self, "_parent")

        if isinstance(parent, TrackedList):
            parent._mark_changed()
        elif isinstance(parent, TrackedObject):
            parent._mark_parent_changed()

    def __repr__(self):
        obj = object.__getattribute__(self, "_obj")
        return repr(obj)


class Session:
    """
    Session в стиле SQLAlchemy с отслеживанием изменений и MongoDB интеграцией

    Каждая сессия создается для одного запроса и имеет собственное состояние.
    """

    # Глобальное хранилище оригинальных __setattr__ для каждого класса
    # Остается глобальным, т.к. модификация класса происходит один раз навсегда
    _original_setattrs: Dict[type, Any] = {}

    def __init__(
            self,
            db: Any,
            mongo_session: Optional[AsyncIOMotorClientSession] = None,
            collection_mapping: Optional[Dict[Type, str]] = None,
    ):
        """
        Инициализация сессии

        Args:
            db: AsyncIOMotorDatabase
            mongo_session: MongoDB session для транзакций (опционально)
            collection_mapping: Маппинг класс -> имя коллекции
                Пример: {User: "users", Article: "blog_articles"}
        """
        self.db = db
        self.mongo_session = mongo_session
        self.collection_mapping = collection_mapping or {}

        # Состояние для этой сессии (не глобальное!)
        self._instance_states: Dict[int, InstanceState] = {}
        self._tracked_instances = []

    def add(self, instance: Any) -> Any:
        """Добавляет экземпляр под отслеживание"""
        cls = instance.__class__
        instance_id = id(instance)

        if instance_id not in self._instance_states:
            self._instance_states[instance_id] = InstanceState(instance)

        if cls not in Session._original_setattrs:
            self._instrument_class(cls)

        self._tracked_instances.append(instance)
        self._wrap_mutable_fields(instance)

        return instance

    def _wrap_mutable_fields(self, instance: Any):
        """Оборачивает mutable поля в отслеживаемые прокси"""
        # Сохраняем weak reference на саму сессию
        object.__setattr__(instance, '_session_ref', ref(self))

        for field_name in dir(instance):
            if field_name.startswith("_"):
                continue
            try:
                value = getattr(instance, field_name)
                if isinstance(value, list) and not isinstance(value, TrackedList):
                    tracked_list = TrackedList(
                        value, instance, field_name, self._instance_states
                    )
                    object.__setattr__(instance, field_name, tracked_list)
            except:
                pass

    def _instrument_class(self, target_class: type):
        """Инструментирует класс для отслеживания изменений"""
        original_setattr = target_class.__setattr__
        Session._original_setattrs[target_class] = original_setattr

        def tracking_setattr(self, name: str, value: Any) -> None:
            instance_id = id(self)

            # Получаем сессию через weak reference
            session_ref = getattr(self, '_session_ref', None)

            if session_ref is not None:
                session = session_ref()
                if session is not None:
                    session_states = session._instance_states
                    if instance_id in session_states and not name.startswith("_"):
                        state = session_states[instance_id]

                        if hasattr(self, name):
                            old_value = getattr(self, name)
                            state.mark_changed(name, old_value)

                        if isinstance(value, list) and not isinstance(value, TrackedList):
                            value = TrackedList(value, self, name, session_states)

            original_setattr(self, name, value)

        target_class.__setattr__ = tracking_setattr

        def get_changed_fields(self) -> Set[str]:
            instance_id = id(self)
            session_ref = getattr(self, '_session_ref', None)

            if session_ref is not None:
                session = session_ref()
                if session is not None and instance_id in session._instance_states:
                    return session._instance_states[instance_id].get_changed_fields()

            return set()

        def get_original_value(self, field_name: str) -> Any:
            instance_id = id(self)
            session_ref = getattr(self, '_session_ref', None)

            if session_ref is not None:
                session = session_ref()
                if session is not None and instance_id in session._instance_states:
                    return session._instance_states[instance_id].get_original_value(field_name)

            return None

        target_class.get_changed_fields = get_changed_fields
        target_class.get_original_value = get_original_value

    def _get_collection_name(self, instance: Any) -> str:
        """Получает имя коллекции для экземпляра"""
        cls = instance.__class__

        # Сначала проверяем маппинг
        if cls in self.collection_mapping:
            return self.collection_mapping[cls]

        # Затем проверяем атрибут класса
        if hasattr(cls, "__collection_name__"):
            return cls.__collection_name__

        # По умолчанию: имя класса + 's'
        return cls.__name__.lower() + "s"

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

    def build_update_query(self, instance: Any) -> Optional[Dict[str, Any]]:
        """
        Создает MongoDB update запрос на основе изменений

        Returns:
            Dict с операторами MongoDB ($set, $unset и т.д.)
        """
        instance_id = id(instance)
        if instance_id not in self._instance_states:
            return None

        state = self._instance_states[instance_id]
        changed_fields = state.get_changed_fields()

        if not changed_fields:
            return None

        update_query = {}
        set_operations = {}

        for field_name in changed_fields:
            if field_name == "_id":
                continue

            current_value = getattr(instance, field_name)
            serialized_value = self._serialize_value(current_value)
            set_operations[field_name] = serialized_value

        if set_operations:
            update_query["$set"] = set_operations

        return update_query if update_query else None

    async def commit(self):
        """
        Сохраняет все изменения в MongoDB в рамках переданной транзакции
        """
        if self.db is None:
            raise ValueError("Database not set.")

        for instance in self._tracked_instances:
            instance_id = id(instance)

            if instance_id not in self._instance_states:
                continue

            state = self._instance_states[instance_id]

            if not state.changed_fields:
                continue

            # Получаем имя коллекции
            collection_name = self._get_collection_name(instance)
            collection: AsyncIOMotorCollection = self.db[collection_name]

            # Строим update запрос
            update_query = self.build_update_query(instance)

            if not update_query:
                continue

            # Получаем _id для фильтра
            doc_id = getattr(instance, "_id", None)

            # Опции для операций с транзакцией
            operation_kwargs = {}
            if self.mongo_session:
                operation_kwargs["session"] = self.mongo_session

            if doc_id:
                # Update существующего документа
                filter_query = {
                    "_id": ObjectId(doc_id) if isinstance(doc_id, str) else doc_id
                }
                result = await collection.update_one(
                    filter_query, update_query, **operation_kwargs
                )
            else:
                # Insert нового документа
                doc_data = self._serialize_value(instance)
                result = await collection.insert_one(doc_data, **operation_kwargs)
                setattr(instance, "_id", result.inserted_id)

            # Очищаем отслеживание после сохранения
            state.changed_fields.clear()
            state.original_values.clear()

    async def flush(self):
        """Синоним для commit (как в SQLAlchemy)"""
        await self.commit()

    def rollback(self):
        """Откатывает изменения"""
        for instance in self._tracked_instances:
            instance_id = id(instance)
            if instance_id in self._instance_states:
                state = self._instance_states[instance_id]
                for field, value in list(state.original_values.items()):
                    if isinstance(value, list):
                        restored = copy.deepcopy(value)
                        tracked = TrackedList(
                            restored, instance, field, self._instance_states
                        )
                        setattr(instance, field, tracked)
                    else:
                        setattr(instance, field, value)
                state.changed_fields.clear()
                state.original_values.clear()

    def close(self):
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

    def __repr__(self):
        return f"Tag({self.title})"


@dataclass
class User:
    username: str
    email: str
    tags: list[Tag] = field(default_factory=list)
    _id: Optional[str] = None


@dataclass
class Article:
    title: str
    content: str
    _id: Optional[str] = None

    # Можно указать имя коллекции напрямую в классе
    __collection_name__ = "blog_articles"


async def example_with_transaction():
    """Пример использования с транзакцией MongoDB"""
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client.mydb

    # Начинаем транзакцию
    async with await client.start_session() as mongo_session:
        async with mongo_session.start_transaction():
            # Создаем сессию ORM с транзакцией
            session = Session(
                db=db,
                mongo_session=mongo_session,
                collection_mapping={
                    User: "users",
                    Article: "blog_articles",  # Кастомное имя
                },
            )

            try:
                # Работаем с объектами
                user = User(
                    username="john",
                    email="john@example.com",
                    tags=[Tag("python")],
                    _id="507f1f77bcf86cd799439011",
                )

                session.add(user)
                user.username = "john_doe"
                user.tags.append(Tag("mongodb"))

                # Сохраняем в рамках транзакции
                await session.commit()

                print("Transaction committed successfully")

            except Exception as e:
                print(f"Error: {e}, transaction will rollback")
                raise
            finally:
                session.close()

    client.close()


async def example_simple_request():
    """Пример для одного API запроса без транзакции"""
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client.mydb

    # Каждый запрос создает свою сессию
    session = Session(db=db)

    try:
        user = User(username="alice", email="alice@example.com")

        session.add(user)
        user.tags.append(Tag("python"))
        user.tags[0].title = "python3"

        print("Changed:", user.get_changed_fields())
        print("Query:", session.build_update_query(user))

        await session.commit()
    finally:
        session.close()

    client.close()


async def example_multiple_sessions():
    """Демонстрация изоляции между сессиями"""
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client.mydb

    user1 = User(username="user1", email="user1@example.com")
    user2 = User(username="user2", email="user2@example.com")

    # Две независимые сессии
    session1 = Session(db=db)
    session2 = Session(db=db)

    try:
        session1.add(user1)
        user1.username = "modified1"

        session2.add(user2)
        user2.username = "modified2"

        # Каждая сессия видит только свои изменения
        print("Session1 changes:", user1.get_changed_fields())  # {'username'}
        print("Session2 changes:", user2.get_changed_fields())  # {'username'}

        await session1.commit()
        await session2.commit()
    finally:
        session1.close()
        session2.close()

    client.close()


async def example_with_rollback():
    """Пример с откатом изменений"""
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client.mydb

    session = Session(db=db)

    try:
        user = User(username="john", email="john@example.com", tags=[Tag("python")])
        session.add(user)

        # Изменяем
        user.username = "jane"
        user.email = "jane@example.com"
        user.tags.append(Tag("mongodb"))

        print("Before rollback:")
        print(f"  Username: {user.username}")
        print(f"  Email: {user.email}")
        print(f"  Tags: {user.tags}")
        print(f"  Changed: {user.get_changed_fields()}")

        # Откатываем изменения
        session.rollback()

        print("\nAfter rollback:")
        print(f"  Username: {user.username}")
        print(f"  Email: {user.email}")
        print(f"  Tags: {user.tags}")
        print(f"  Changed: {user.get_changed_fields()}")

    finally:
        session.close()

    client.close()


async def example_api_endpoint_pattern():
    """
    Типичный паттерн использования в FastAPI endpoint
    """
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def get_session(db, mongo_session=None):
        """Helper для создания сессии с автоматическим cleanup"""
        session = Session(db=db, mongo_session=mongo_session)
        try:
            yield session
        finally:
            session.close()

    # Использование в endpoint
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client.mydb

    # Вариант 1: Без транзакции
    async with get_session(db) as session:
        user = User(username="api_user", email="api@example.com")
        session.add(user)
        user.username = "updated_user"
        await session.commit()
        print(f"User saved with _id: {user._id}")

    # Вариант 2: С транзакцией
    async with await client.start_session() as mongo_session:
        async with mongo_session.start_transaction():
            async with get_session(db, mongo_session) as session:
                user = User(username="transactional_user", email="trans@example.com")
                session.add(user)
                user.username = "updated_trans_user"
                await session.commit()
                print(f"User saved in transaction with _id: {user._id}")

    client.close()


if __name__ == "__main__":
    import asyncio

    print("=== Example 1: Simple Request ===")
    asyncio.run(example_simple_request())

    print("\n=== Example 2: With Transaction ===")
    asyncio.run(example_with_transaction())

    print("\n=== Example 3: Multiple Sessions ===")
    asyncio.run(example_multiple_sessions())

    print("\n=== Example 4: With Rollback ===")
    asyncio.run(example_with_rollback())

    print("\n=== Example 5: API Endpoint Pattern ===")
    asyncio.run(example_api_endpoint_pattern())
