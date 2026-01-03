from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter
from starlette import status

from app.application.interactors.developer.create_developer import (
    CreateDeveloperInteractor,
    CreateDeveloperRequest,
)
from app.application.interactors.developer.delete_developer import (
    DeleteDeveloperInteractor,
    DeleteDeveloperRequest,
)
from app.application.interactors.developer.get_developer import (
    GetDeveloperInteractor,
    GetDeveloperRequest,
)
from app.application.interactors.developer.get_developers import (
    GetDevelopersInteractor,
)
from app.application.interactors.developer.update_developer import (
    UpdateDeveloperInteractor,
    UpdateDeveloperRequest,
)
from app.domain.developer import Developer
from app.presentation.api.developer.schema import (
    CreateDeveloperRequestSchema,
    UpdateDeveloperRequestSchema,
)

developer_router = APIRouter()


@developer_router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
)
@inject
async def create_developer(
    request_data: CreateDeveloperRequestSchema,
    interactor: FromDishka[CreateDeveloperInteractor],
) -> Developer:
    data = CreateDeveloperRequest(
        username=request_data.username,
        full_name=request_data.full_name,
        city=request_data.city,
        country=request_data.country,
        coordinates=request_data.coordinates,
        languages=request_data.languages,
        tags=request_data.tags,
        skills=request_data.skills,
        projects=request_data.projects,
        metadata=request_data.metadata,
    )

    return await interactor(data)


@developer_router.get(
    "/{developer_id}",
    status_code=status.HTTP_200_OK,
)
@inject
async def get_developer(
    developer_id: str,
    interactor: FromDishka[GetDeveloperInteractor],
) -> Developer | None:
    """
    Get developer by ID

    Returns complete developer profile with all nested structures
    """
    request_data = GetDeveloperRequest(developer_id=developer_id)

    return await interactor(request_data)


@developer_router.get(
    "/",
    status_code=status.HTTP_200_OK,
)
@inject
async def get_developers(
    interactor: FromDishka[GetDevelopersInteractor],
) -> list[Developer]:
    """
    Get developers

    Returns complete developer profile with all nested structures
    """
    return await interactor()


@developer_router.patch(
    "/{developer_id}",
    status_code=status.HTTP_200_OK,
)
@inject
async def update_developer(
    developer_id: str,
    request_schema: UpdateDeveloperRequestSchema,
    interactor: FromDishka[UpdateDeveloperInteractor],
) -> Developer | None:
    """
    Update developer by ID

    Updates only provided fields, leaving others unchanged.
    Supports partial updates of nested structures.
    """
    request_data = UpdateDeveloperRequest(
        developer_id=developer_id,
        username=request_schema.username,
        full_name=request_schema.full_name,
        city=request_schema.city,
        country=request_schema.country,
        coordinates=request_schema.coordinates,
        languages=request_schema.languages,
        tags=request_schema.tags,
        skills=request_schema.skills,
        projects=request_schema.projects,
        metadata=request_schema.metadata,
    )

    return await interactor(request_data)


@developer_router.delete(
    "/{developer_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
@inject
async def delete_developer(
        developer_id: str,
        interactor: FromDishka[DeleteDeveloperInteractor],
) -> None:
    """
    Delete developer by ID

    Permanently removes developer from the database.
    Returns 404 if developer not found.
    """
    request_data = DeleteDeveloperRequest(developer_id=developer_id)

    await interactor(request_data)
