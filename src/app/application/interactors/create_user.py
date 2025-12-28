import logging
from dataclasses import dataclass, field

from app.application.user_repo import UserRepository
from app.domain.model import Tag, User

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class CreateUserRequest:
    username: str
    email: str
    tags: list[Tag] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class CreateUserResponse:
    username: str
    email: str


@dataclass(slots=True, frozen=True)
class CreateUserInteractor:
    user_repository: UserRepository

    async def __call__(self, request_data: CreateUserRequest) -> CreateUserResponse:
        logger.info(f"Creating user: {request_data.username}")

        user = User(
            username=request_data.username,
            email=request_data.email,
            tags=request_data.tags,
        )

        await self.user_repository.add(user)

        logger.info(f"User created: {user.username}")

        return CreateUserResponse(
            username=user.username,
            email=user.email,
        )
