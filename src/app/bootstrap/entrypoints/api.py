from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from adaptix import P, Retort, loader, validator
from bson import ObjectId
from dishka.integrations.fastapi import setup_dishka
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from motor.motor_asyncio import AsyncIOMotorClient
from starlette_admin import BaseAdmin

from app.bootstrap.configs import load_settings
from app.bootstrap.ioc.containers import fastapi_container
from app.domain.course import Course
from app.domain.model import User
from app.example import instrument_class
from app.infrastructure.log.main import configure_logging
from app.presentation.admin.mongo_course_view import MongoCourseView
from app.presentation.admin.mongo_view import MongoUserView
from app.presentation.api.middlewares.setup import setup_middlewares
from app.presentation.api.root import root_router
from app.presentation.exceptions import setup_exception_handlers


def init_routers(app: FastAPI) -> None:
    app.include_router(root_router)
    setup_exception_handlers(app)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    instrument_class(User)
    yield
    await app.state.dishka_container.close()


def create_app() -> FastAPI:
    load_dotenv()
    configure_logging()
    config = load_settings()

    app = FastAPI(lifespan=lifespan, default_response_class=ORJSONResponse)

    admin = BaseAdmin(
        title="User Management",
        base_url="/admin",
    )
    admin.add_view(MongoUserView())
    admin.mount_to(app)
    client: AsyncIOMotorClient[dict[str, Any]] = AsyncIOMotorClient(
        config.database.uri,
    )
    course_view = MongoCourseView(
        client=client,
        retort=Retort(
            recipe=[
                loader(
                    P._id,  # noqa: SLF001
                    lambda x: str(x) if isinstance(x, ObjectId) else x,
                ),
                validator(
                    P[Course].price,
                    lambda x: x >= 0,
                    "Цена не может быть отрицательной",
                ),
            ],
        ),
    )
    admin.add_view(course_view)
    init_routers(app)
    setup_middlewares(app)
    container = fastapi_container(config)
    setup_dishka(container=container, app=app)

    return app
