from dataclasses import dataclass

from app.domain.common.exceptions import AppError


@dataclass(eq=False)
class ApplicationError(AppError):

    @property
    def title(self) -> str:
        return "An application error occurred"
