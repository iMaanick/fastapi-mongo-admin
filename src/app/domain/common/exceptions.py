from dataclasses import dataclass


@dataclass(eq=False)
class AppError(Exception):
    @property
    def message(self) -> str:
        return ""

    @property
    def title(self) -> str:
        return "An app error occurred"


@dataclass(eq=False)
class DomainError(AppError):

    @property
    def title(self) -> str:
        return "A domain error occurred"
