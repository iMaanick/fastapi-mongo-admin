import logging

from dishka import AsyncContainer, make_async_container

from app.bootstrap.configs import Config, MongoDBConfig
from app.bootstrap.ioc.application import ApplicationProvider
from app.bootstrap.ioc.infrastructure import InfrastructureProvider

logger = logging.getLogger(__name__)


def fastapi_container(
        config: Config,
) -> AsyncContainer:
    logger.info("Fastapi DI setup")

    return make_async_container(
        InfrastructureProvider(),
        ApplicationProvider(),
        context={
            MongoDBConfig: config.database,
        },
    )
