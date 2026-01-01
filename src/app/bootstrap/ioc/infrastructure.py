import logging
from collections.abc import AsyncIterator
from typing import Any

from adaptix import P, Retort, loader, name_mapping
from bson import ObjectId
from dishka import Provider, Scope, provide
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

from app.application.change_tracker import ChangeTracker
from app.bootstrap.configs import MongoDBConfig
from app.domain.model import User
from app.example import Session
from app.infrastructure.db.change_tracker import MongoChangeTracker

logger = logging.getLogger(__name__)


class InfrastructureProvider(Provider):
    scope = Scope.REQUEST

    @provide(scope=Scope.APP)
    async def get_mongo_client(
        self,
        config: MongoDBConfig,
    ) -> AsyncIterator[AsyncIOMotorClient[dict[str, Any]]]:
        client: AsyncIOMotorClient[dict[str, Any]] = AsyncIOMotorClient(
            config.uri,
        )
        logger.debug("MongoDB client was initialized")
        yield client
        client.close()
        logger.debug("MongoDB client was closed")

    @provide(scope=Scope.APP)
    def get_database(
        self,
        client: AsyncIOMotorClient[dict[str, Any]],
        config: MongoDBConfig,
    ) -> AsyncIOMotorDatabase[dict[str, Any]]:
        database = client[config.db_name]
        logger.debug("Database '%s' was initialized", config.db_name)
        return database

    @provide
    def get_collection(
        self,
        database: AsyncIOMotorDatabase[dict[str, Any]],
        config: MongoDBConfig,
    ) -> AsyncIOMotorCollection[dict[str, Any]]:
        return database[config.collection_name]

    @provide(scope=Scope.REQUEST)
    async def get_session(
        self,
        client: AsyncIOMotorClient[dict[str, Any]],
    ) -> AsyncIterator[AsyncIOMotorClientSession]:
        """Автоматически оборачивает весь REQUEST в транзакцию."""
        async with (
            await client.start_session() as session,
            session.start_transaction(),
        ):
            logger.debug("MongoDB transaction started")
            yield session
            if session.in_transaction:  # type: ignore[truthy-function]
                # нужно чтобы не было автокоммита
                await session.abort_transaction()
            logger.debug("MongoDB transaction committed")

    @provide(scope=Scope.APP)
    def get_mongo_retort(self) -> Retort:
        return Retort(
            recipe=[
                loader(
                    P[User]._id,  # noqa: SLF001
                    lambda x: str(x) if isinstance(x, ObjectId) else x,
                ),
                name_mapping(
                    User,
                    skip=["unused_field"],  # Пропускаем при dump И при load
                ),
            ],
        )

    @provide(scope=Scope.REQUEST)
    def get_change_tracker(
        self,
        database: AsyncIOMotorDatabase[dict[str, Any]],
        retort: Retort,
        session: AsyncIOMotorClientSession,
    ) -> ChangeTracker:
        return MongoChangeTracker(
            collection_mapping={User: "example"},
            database=database,
            retort=retort,
            session=session,
        )

    @provide(scope=Scope.REQUEST)
    def get_new_change_tracker(
        self,
        database: AsyncIOMotorDatabase[dict[str, Any]],
        session: AsyncIOMotorClientSession,
    ) -> Session:
        return Session(
            db=database,
            mongo_session=session,
            collection_mapping={
                User: "example",
            },
        )