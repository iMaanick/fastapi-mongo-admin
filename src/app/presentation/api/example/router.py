from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter

from app.application.interactors.create_user import (
    CreateUserInteractor,
    CreateUserRequest,
    CreateUserResponse,
)
from app.application.interactors.get_user import (
    GetUserInteractor,
    GetUserRequest,
)
from app.application.interactors.update_user import (
    UpdateUserInteractor,
    UpdateUserRequest,
)
from app.application.interactors.update_users import (
    UpdateUsersInteractor,
    UpdateUsersResponse,
)
from app.domain.model import User
from app.presentation.api.example.schema import UpdateUserBody

example_router = APIRouter()


@example_router.post("/create_user")
@inject
async def create_user(
        request_data: CreateUserRequest,
        interactor: FromDishka[CreateUserInteractor],
) -> CreateUserResponse:
    return await interactor(request_data)


@example_router.get("/users/{user_id}")
@inject
async def get_user(
        user_id: str,
        interactor: FromDishka[GetUserInteractor],
) -> User | None:
    return await interactor(
        GetUserRequest(user_id=user_id),
    )


@example_router.post("/users/bulk-update")
@inject
async def update_users_bulk(
        interactor: FromDishka[UpdateUsersInteractor],
) -> UpdateUsersResponse:
    return await interactor()


@example_router.put("/users/{user_id}")
@inject
async def update_user(
        user_id: str,
        body: UpdateUserBody,
        interactor: FromDishka[UpdateUserInteractor],
) -> User:
    request_data = UpdateUserRequest(
        user_id=user_id,
        username=body.username,
        email=body.email,
        tags=body.tags,
        is_active=body.is_active,
    )

    return await interactor(request_data)
