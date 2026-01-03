from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter
from starlette import status

from app.application.interactors.developer.create_developer import (
    CreateDeveloperInteractor,
    CreateDeveloperRequest,
)
from app.application.interactors.developer.get_developer import (
    GetDeveloperInteractor,
    GetDeveloperRequest,
)
from app.presentation.api.developer.schema import CreateDeveloperRequestSchema

developer_router = APIRouter()


@developer_router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
)
@inject
async def create_developer(
    request_data: CreateDeveloperRequestSchema,
    interactor: FromDishka[CreateDeveloperInteractor],
):
    request_data = CreateDeveloperRequest(
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

    return await interactor(request_data)


@developer_router.get(
    "/{developer_id}",
    status_code=status.HTTP_200_OK,
)
@inject
async def get_developer(
    developer_id: str,
    interactor: FromDishka[GetDeveloperInteractor],
):
    """
    Get developer by ID

    Returns complete developer profile with all nested structures
    """
    request_data = GetDeveloperRequest(developer_id=developer_id)

    return await interactor(request_data)
