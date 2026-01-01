import copy
from dataclasses import fields, is_dataclass
from typing import Any, SupportsIndex
from weakref import ref

from bson import ObjectId
from motor.motor_asyncio import (
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
            if isinstance(original_value, TrackedObject):
                obj = object.__getattribute__(original_value, "_obj")
                self.original_values[field_name] = copy.deepcopy(obj)
            elif isinstance(original_value, list | dict):
                if hasattr(original_value, "_unwrap_for_copy"):
                    unwrapper = original_value._unwrap_for_copy  # noqa: SLF001
                    self.original_values[field_name] = unwrapper(
                        original_value,
                    )
                else:
                    self.original_values[field_name] = copy.deepcopy(
                        original_value,
                    )
            else:
                self.original_values[field_name] = copy.deepcopy(
                    original_value,
                )
        self.changed_fields.add(field_name)

    def get_changed_fields(self) -> set[str]:
        return self.changed_fields.copy()

    def get_original_value(self, field_name: str) -> Any:
        return self.original_values.get(field_name)


class TrackedList(list):
    """Список с отслеживанием изменений"""

    def __init__(
        self,
        data: list[Any],
        instance: Any,
        field_name: str,
        session_instance_states: dict[int, InstanceState],
        *,
        is_nested: bool = False,
    ) -> None:
        processed_data = self._wrap_nested_lists(
            data,
            instance,
            field_name,
            session_instance_states,
        )
        super().__init__(processed_data)
        self._instance_id = id(instance)
        self._field_name = field_name
        self._session_instance_states = session_instance_states
        self._is_nested = is_nested

        self._wrap_objects_in_list()

        if not is_nested:
            self._mark_initial_state()

    @staticmethod
    def _wrap_nested_lists(
        data: list[Any],
        instance: Any,
        field_name: str,
        session_instance_states: dict[int, InstanceState],
    ) -> list[Any]:
        """Рекурсивно оборачивает вложенные списки"""
        result = []
        for item in data:
            if isinstance(item, list) and not isinstance(item, TrackedList):
                result.append(
                    TrackedList(
                        item,
                        instance,
                        field_name,
                        session_instance_states,
                        is_nested=True,
                    ),
                )
            else:
                result.append(item)
        return result

    def _wrap_objects_in_list(self) -> None:
        """Оборачивает объекты в TrackedObject"""
        for i, item in enumerate(self):
            if isinstance(item, (TrackedObject, TrackedList)):
                continue
            if hasattr(item, "__dict__") and not isinstance(item, list):
                super().__setitem__(i, TrackedObject(item, self))

    def _mark_initial_state(self) -> None:
        if self._instance_id in self._session_instance_states:
            state = self._session_instance_states[self._instance_id]
            if self._field_name not in state.original_values:
                unwrapped = self._unwrap_for_copy(self)
                state.original_values[self._field_name] = unwrapped

    def _unwrap_for_copy(self, data: Any) -> Any:
        """Разворачивает TrackedList/TrackedObject"""
        if isinstance(data, TrackedList):
            return [self._unwrap_for_copy(item) for item in data]
        if isinstance(data, TrackedObject):
            obj = object.__getattribute__(data, "_obj")
            return copy.deepcopy(obj)
        if isinstance(data, list):
            return [self._unwrap_for_copy(item) for item in data]
        return copy.copy(data) if hasattr(data, "__dict__") else data

    def _mark_changed(self) -> None:
        if self._instance_id in self._session_instance_states:
            state = self._session_instance_states[self._instance_id]
            state.changed_fields.add(self._field_name)

    def append(self, item: Any) -> None:
        self._mark_changed()
        if isinstance(item, list) and not isinstance(item, TrackedList):
            item = TrackedList(
                item,
                self._get_root_instance(),
                self._field_name,
                self._session_instance_states,
                is_nested=True,
            )
        elif hasattr(item, "__dict__") and not isinstance(
            item,
            TrackedObject,
        ):
            item = TrackedObject(item, self)
        super().append(item)

    def extend(self, items: Any) -> None:
        self._mark_changed()
        wrapped_items: list[Any] = []
        for item in items:
            if isinstance(item, list) and not isinstance(
                item,
                TrackedList,
            ):
                wrapped_items.append(
                    TrackedList(
                        item,
                        self._get_root_instance(),
                        self._field_name,
                        self._session_instance_states,
                        is_nested=True,
                    ),
                )
            elif hasattr(item, "__dict__") and not isinstance(
                item,
                TrackedObject,
            ):
                wrapped_items.append(TrackedObject(item, self))
            else:
                wrapped_items.append(item)
        super().extend(wrapped_items)

    def insert(self, index: SupportsIndex, item: Any) -> None:
        self._mark_changed()
        if isinstance(item, list) and not isinstance(item, TrackedList):
            item = TrackedList(
                item,
                self._get_root_instance(),
                self._field_name,
                self._session_instance_states,
                is_nested=True,
            )
        elif hasattr(item, "__dict__") and not isinstance(
            item,
            TrackedObject,
        ):
            item = TrackedObject(item, self)
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
        if isinstance(value, list) and not isinstance(value, TrackedList):
            value = TrackedList(
                value,
                self._get_root_instance(),
                self._field_name,
                self._session_instance_states,
                is_nested=True,
            )
        elif hasattr(value, "__dict__") and not isinstance(
            value,
            TrackedObject,
        ):
            value = TrackedObject(value, self)
        super().__setitem__(key, value)

    def __delitem__(self, key: Any) -> None:
        self._mark_changed()
        super().__delitem__(key)

    def __getitem__(self, key: Any) -> Any:
        item = super().__getitem__(key)

        if isinstance(item, (TrackedList, TrackedObject)):
            return item

        if isinstance(item, list):
            tracked_list = TrackedList(
                item,
                self._get_root_instance(),
                self._field_name,
                self._session_instance_states,
                is_nested=True,
            )
            super().__setitem__(key, tracked_list)
            return tracked_list

        if hasattr(item, "__dict__") and not isinstance(item, list):
            tracked_object = TrackedObject(item, self)
            super().__setitem__(key, tracked_object)
            return tracked_object

        return item

    def _get_root_instance(self) -> Any:
        """Получает корневой instance"""
        current = self
        while isinstance(current, TrackedList):
            inst_id = current._instance_id  # noqa: SLF001
            states = current._session_instance_states  # noqa: SLF001
            if inst_id in states:
                state = states[inst_id]
                if state.instance_ref() is not None:
                    return state.instance_ref()
            break
        return None


class TrackedObject:
    """Прокси для отслеживания изменений вложенных объектов"""

    def __init__(self, obj: Any, parent: Any) -> None:
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "_parent", parent)

    def __getattribute__(self, name: str) -> Any:
        if name == "__class__":
            obj = object.__getattribute__(self, "_obj")
            return obj.__class__

        if name in ("_obj", "_parent"):
            return object.__getattribute__(self, name)

        if name in (
            "_mark_parent_changed",
            "_mark_root_instance_changed",
            "_find_field_name_in_parent",
            "_get_root_instance",
            "_get_root_field_name",
            "__setattr__",
            "__getitem__",
            "__repr__",
        ):
            return object.__getattribute__(self, name)

        obj = object.__getattribute__(self, "_obj")
        value = getattr(obj, name)

        if hasattr(value, "__dict__") and not isinstance(
            value,
            TrackedObject,
        ):
            return TrackedObject(value, self)

        if isinstance(value, list) and not isinstance(value, TrackedList):
            root_instance = self._get_root_instance()
            field_name = self._get_root_field_name()

            if root_instance is not None:
                session_ref = getattr(root_instance, "_session_ref", None)
                if session_ref is not None:
                    session = session_ref()
                    if session is not None:
                        tracked_list = TrackedList(
                            value,
                            root_instance,
                            field_name,
                            session._instance_states,  # noqa: SLF001
                            is_nested=True,
                        )
                        setattr(obj, name, tracked_list)
                        return tracked_list

        return value

    def __getitem__(self, key: Any) -> Any:
        """Поддержка индексации"""
        obj = object.__getattribute__(self, "_obj")
        if hasattr(obj, "__getitem__"):
            return obj[key]
        msg = f"'{type(obj).__name__}' object is not subscriptable"
        raise TypeError(msg)

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
        else:
            self._mark_root_instance_changed()

    def _mark_root_instance_changed(self) -> None:
        """Отмечает изменение в корневом instance"""
        parent = object.__getattribute__(self, "_parent")

        if not hasattr(parent, "_session_ref"):
            return

        session_ref = getattr(parent, "_session_ref", None)
        if session_ref is None:
            return

        session = session_ref()
        if session is None:
            return

        instance_id = id(parent)
        if instance_id not in session._instance_states:  # noqa: SLF001
            return

        field_name = self._find_field_name_in_parent(parent)
        if field_name:
            state = session._instance_states[instance_id]  # noqa: SLF001
            state.changed_fields.add(field_name)

    def _find_field_name_in_parent(self, parent: Any) -> str | None:
        """Находит имя поля в parent"""
        obj = object.__getattribute__(self, "_obj")

        for field_name in dir(parent):
            if field_name.startswith("_"):
                continue
            try:
                value = getattr(parent, field_name)

                if isinstance(value, TrackedObject):
                    wrapped_obj = object.__getattribute__(value, "_obj")
                    if wrapped_obj is obj or value is self:
                        return field_name
                elif value is obj:
                    return field_name

            except (AttributeError, TypeError):
                pass

        return None

    def __repr__(self) -> str:
        obj = object.__getattribute__(self, "_obj")
        return repr(obj)

    def _get_root_instance(self) -> Any:
        """Получает корневой instance"""
        parent = object.__getattribute__(self, "_parent")

        if isinstance(parent, TrackedList):
            return parent._get_root_instance()  # noqa: SLF001
        if isinstance(parent, TrackedObject):
            return parent._get_root_instance()  # noqa: SLF001

        return None

    def _get_root_field_name(self) -> str:
        """Получает имя поля корневого списка"""
        parent = object.__getattribute__(self, "_parent")

        if isinstance(parent, TrackedList):
            return parent._field_name  # noqa: SLF001
        if isinstance(parent, TrackedObject):
            return parent._get_root_field_name()  # noqa: SLF001

        return ""


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


_INSTRUMENTED_MARKER = "__session_instrumented__"


def is_class_instrumented(cls: type) -> bool:
    """Проверяет, инструментирован ли класс"""
    return hasattr(cls, _INSTRUMENTED_MARKER)


def instrument_class(cls: type) -> None:  # noqa: C901
    """Инструментирует класс для отслеживания изменений"""
    if is_class_instrumented(cls):
        raise ClassAlreadyInstrumentedError(cls)

    original_setattr = cls.__setattr__

    def tracking_setattr(instance: Any, name: str, value: Any) -> None:
        instance_id = id(instance)
        session_ref = getattr(instance, "_session_ref", None)

        if session_ref is not None:
            session = session_ref()
            if session is not None:
                states = session._instance_states  # noqa: SLF001
                if instance_id in states and not name.startswith("_"):
                    state = states[instance_id]

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
                            states,
                        )

        original_setattr(instance, name, value)  # type: ignore[call-arg]

    def get_changed_fields(self: Any) -> set[str]:
        instance_id = id(self)
        session_ref = getattr(self, "_session_ref", None)

        if session_ref is not None:
            session = session_ref()
            if session is not None:
                states = session._instance_states  # noqa: SLF001
                if instance_id in states:
                    return states[instance_id].get_changed_fields()
        return set()

    def get_original_value(self: Any, field_name: str) -> Any:
        instance_id = id(self)
        session_ref = getattr(self, "_session_ref", None)

        if session_ref is not None:
            session = session_ref()
            if session is not None:
                states = session._instance_states  # noqa: SLF001
                if instance_id in states:
                    return states[instance_id].get_original_value(field_name)
        return None

    cls.__setattr__ = tracking_setattr  # type: ignore[assignment]
    cls.get_changed_fields = get_changed_fields  # type: ignore[attr-defined]
    cls.get_original_value = get_original_value  # type: ignore[attr-defined]

    setattr(cls, _INSTRUMENTED_MARKER, True)


class Session:
    """Session в стиле SQLAlchemy с отслеживанием изменений"""

    def __init__(
        self,
        db: AsyncIOMotorDatabase[dict[str, Any]],
        mongo_session: AsyncIOMotorClientSession | None = None,
        collection_mapping: dict[type, str] | None = None,
    ) -> None:
        self.db = db
        self.mongo_session = mongo_session
        self.collection_mapping = collection_mapping or {}
        self._instance_states: dict[int, InstanceState] = {}
        self._tracked_instances: list[Any] = []

    def add(self, instance: Any) -> Any:
        """Добавляет экземпляр под отслеживание"""
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
        """Оборачивает mutable поля"""
        object.__setattr__(instance, "_session_ref", ref(self))

        instance_id = id(instance)

        for field_name in dir(instance):
            if field_name.startswith("_"):
                continue
            try:
                value = getattr(instance, field_name)

                is_list = isinstance(value, list)
                not_tracked = not isinstance(value, TrackedList)
                if is_list and not_tracked:
                    tracked_list = TrackedList(
                        value,
                        instance,
                        field_name,
                        self._instance_states,
                    )
                    object.__setattr__(instance, field_name, tracked_list)

                elif (
                    hasattr(value, "__dict__")
                    and not isinstance(value, TrackedObject)
                    and is_dataclass(value)
                ):
                    state = self._instance_states[instance_id]
                    if field_name not in state.original_values:
                        original = copy.deepcopy(value)
                        state.original_values[field_name] = original

                    tracked_obj = TrackedObject(value, instance)
                    object.__setattr__(instance, field_name, tracked_obj)

            except (AttributeError, TypeError):
                pass

    def _get_collection_name(self, instance: Any) -> str:
        """Получает имя коллекции"""
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
        """Создает MongoDB update запрос"""
        instance_id = id(instance)
        if instance_id not in self._instance_states:
            return None

        state = self._instance_states[instance_id]
        changed_fields = state.get_changed_fields()

        if not changed_fields:
            return None

        set_operations = {
            field_name: self._serialize_value(getattr(instance, field_name))
            for field_name in changed_fields
            if field_name != "_id"
        }

        return {"$set": set_operations} if set_operations else None

    async def commit(self) -> None:
        """Сохраняет все изменения в MongoDB"""
        if self.db is None:
            raise DatabaseNotSetError

        try:
            await self.flush()

            if self.mongo_session and self.mongo_session.in_transaction:  # type: ignore[truthy-function]
                await self.mongo_session.commit_transaction()
        except Exception:
            if self.mongo_session and self.mongo_session.in_transaction:  # type: ignore[truthy-function]
                await self.mongo_session.abort_transaction()
            raise

    async def _commit_instance(self, instance: Any) -> None:
        """Сохраняет изменения одного экземпляра"""
        instance_id = id(instance)

        if instance_id not in self._instance_states:
            return

        state = self._instance_states[instance_id]
        if not state.changed_fields:
            return

        collection_name = self._get_collection_name(instance)
        coll: AsyncIOMotorCollection[dict[str, Any]]
        coll = self.db[collection_name]

        update_query = self.build_update_query(instance)
        if not update_query:
            return

        doc_id = getattr(instance, "_id", None)

        if doc_id:
            await self._update_document(
                collection=coll,
                doc_id=doc_id,
                update_query=update_query,
                session=self.mongo_session,
            )
        else:
            await self._insert_document(
                collection=coll,
                instance=instance,
                session=self.mongo_session,
            )

        state.changed_fields.clear()
        state.original_values.clear()

    async def _update_document(
        self,
        collection: AsyncIOMotorCollection[dict[str, Any]],
        doc_id: str | ObjectId,
        update_query: dict[str, Any],
        session: AsyncIOMotorClientSession | None = None,
    ) -> None:
        """Обновляет существующий документ"""
        filter_query = {
            "_id": ObjectId(doc_id) if isinstance(doc_id, str) else doc_id,
        }

        if session is not None:
            await collection.update_one(
                filter_query,
                update_query,
                session=session,
            )
        else:
            await collection.update_one(
                filter_query,
                update_query,
            )

    async def _insert_document(
        self,
        collection: AsyncIOMotorCollection[dict[str, Any]],
        instance: Any,
        session: AsyncIOMotorClientSession | None = None,
    ) -> None:
        """Вставляет новый документ"""
        doc_data = self._serialize_value(instance)

        if "_id" in doc_data and doc_data["_id"] is None:
            del doc_data["_id"]

        if session is not None:
            result = await collection.insert_one(doc_data, session=session)
        else:
            result = await collection.insert_one(doc_data)

        object.__setattr__(instance, "_id", result.inserted_id)

    async def flush(self) -> None:
        """Сохраняет изменения в БД"""
        if self.db is None:
            raise DatabaseNotSetError

        for instance in self._tracked_instances:
            await self._commit_instance(instance)

    async def rollback(self) -> None:
        """Откатывает изменения"""
        for instance in self._tracked_instances:
            self._rollback_instance(instance)

        if self.mongo_session and self.mongo_session.in_transaction:  # type: ignore[truthy-function]
            await self.mongo_session.abort_transaction()

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
