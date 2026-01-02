from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter
from starlette import status

from app.application.interactors.developer.create_developer import (
    CreateDeveloperInteractor,
    CreateDeveloperRequest,
)
from app.domain.developer import Developer
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
