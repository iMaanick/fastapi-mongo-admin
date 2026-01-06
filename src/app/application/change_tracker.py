import logging
from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol, TypeVar

from app.application.exceptions.base import ApplicationError

logger = logging.getLogger(__name__)


@dataclass(eq=False)
class ChangeTrackerError(ApplicationError):
    """Базовое исключение для ChangeTracker"""

    @property
    def message(self) -> str:
        return "Change tracker error occurred"


@dataclass(eq=False)
class CollectionMappingNotFoundError(ChangeTrackerError):
    """Исключение когда не найден маппинг коллекции"""

    entity_type: type

    @property
    def message(self) -> str:
        return f"No collection mapping found for {self.entity_type.__name__}"


@dataclass(eq=False)
class InvalidEntityIdError(ChangeTrackerError):
    """Исключение когда entity_id невалиден"""

    entity_id: str
    entity_type: type

    @property
    def message(self) -> str:
        # E501 - разбиваем длинную строку
        entity_name = self.entity_type.__name__
        return f"Invalid entity ID '{self.entity_id}' for {entity_name}"


@dataclass(eq=False)
class NoValidOperationsError(ChangeTrackerError):
    """Исключение когда нет валидных операций для сохранения"""

    entity_type: type

    @property
    def message(self) -> str:
        return f"No valid operations to save for {self.entity_type.__name__}"


@dataclass(eq=False)
class TransactionNotActiveError(ChangeTrackerError):
    """Исключение когда транзакция не активна"""

    @property
    def message(self) -> str:
        return "No active transaction to perform this operation"


@dataclass(eq=False)
class EntityNotDataclassError(ChangeTrackerError):
    """Исключение когда entity не является dataclass"""

    entity_type: type

    @property
    def message(self) -> str:
        return f"{self.entity_type.__name__} must be a dataclass"


@dataclass(eq=False)
class EntityMissingIdError(ChangeTrackerError):
    """Исключение когда у entity отсутствует _id"""

    entity_type: type

    @property
    def message(self) -> str:
        return f"Cannot track {self.entity_type.__name__} without _id"


@dataclass(eq=False)
class InvalidRequestError(ChangeTrackerError):
    """Исключение когда у entity отсутствует _id"""

    entity_type: type
    entity_id: str

    @property
    def message(self) -> str:
        return (
            f"Can't attach {self.entity_type.__name__} "
            f"with id={self.entity_id}: "
            f"another instance with the same id is already tracked"
        )


@dataclass(eq=False)
class OriginalSnapshotNotFoundError(ChangeTrackerError):
    """Исключение, когда не найден оригинальный снапшот сущности"""

    entity_type: type
    entity_id: str

    @property
    def message(self) -> str:
        return (
            f"No original snapshot found for "
            f"{self.entity_type.__name__} with ID '{self.entity_id}'"
        )


T = TypeVar("T")


class ChangeTracker(Protocol):
    """Protocol для отслеживания изменений любых доменных entities"""

    @abstractmethod
    def track(self, entity: T) -> None:
        """Отследить изменения одной entity (User, Course, Order и т.д.)"""
        raise NotImplementedError

    @abstractmethod
    def track_all(self, entities: list[T]) -> None:
        """Отследить изменения списка entities"""
        raise NotImplementedError

    @abstractmethod
    async def save(self) -> int:
        """Сохранить все отслеживаемые entities"""
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        """Очистить отслеживаемые entities"""
        raise NotImplementedError

    @abstractmethod
    async def commit(self) -> None:
        """Закоммитить транзакцию"""
        raise NotImplementedError

    @abstractmethod
    async def rollback(self) -> None:
        """Откатить транзакцию"""
        raise NotImplementedError
