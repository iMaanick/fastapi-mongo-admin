from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from dishka.integrations.fastapi import setup_dishka
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from starlette_admin import BaseAdmin

from app.bootstrap.configs import load_settings
from app.bootstrap.ioc.containers import fastapi_container
from app.infrastructure.log.main import configure_logging
from app.presentation.admin.mongo_view import MongoUserView
from app.presentation.api.middlewares.setup import setup_middlewares
from app.presentation.api.root import root_router
from app.presentation.exceptions import setup_exception_handlers


def init_routers(app: FastAPI) -> None:
    app.include_router(root_router)
    setup_exception_handlers(app)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    yield
    await app.state.dishka_container.close()


def create_app() -> FastAPI:
    load_dotenv()
    configure_logging()
    app = FastAPI(lifespan=lifespan, default_response_class=ORJSONResponse)

    admin = BaseAdmin(
        title="User Management",
        base_url="/admin",
    )
    admin.add_view(MongoUserView())
    admin.mount_to(app)
    init_routers(app)
    setup_middlewares(app)
    config = load_settings()
    container = fastapi_container(config)
    setup_dishka(container=container, app=app)

    return app
