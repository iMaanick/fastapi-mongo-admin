from dataclasses import dataclass


@dataclass(eq=False)
class AppError(Exception):
    @property
    def message(self) -> str:
        return ""


@dataclass(eq=False)
class DomainError(AppError):

    @property
    def message(self) -> str:
        return "A domain error occurred"
