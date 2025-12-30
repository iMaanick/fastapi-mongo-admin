from dataclasses import dataclass
from typing import Any

from app.domain.common.exceptions import AppError


@dataclass(eq=False)
class ApplicationError(AppError):

    @property
    def message(self) -> str:
        return "An application error occurred"


@dataclass(eq=False)
class EntityNotFoundError(ApplicationError):
    """Исключение когда сущность не найдена"""

    entity_type: type
    field_name: str | None = None
    field_value: Any = None

    @property
    def message(self) -> str:
        entity_name = self.entity_type.__name__

        if self.field_name is None:
            return f"{entity_name} not found"

        return f"{entity_name} not found by {self.field_name}='{self.field_value}'"  # noqa: E501


@dataclass(eq=False)
class InvalidQueryOperatorError(ApplicationError):
    """Исключение когда оператор запроса используется без контекста поля"""

    operator: str

    @property
    def message(self) -> str:
        return f"Operator '{self.operator}' without field context"
