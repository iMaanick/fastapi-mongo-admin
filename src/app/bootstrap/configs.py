from dataclasses import dataclass
from os import environ


@dataclass
class MissingDatabaseConfigError(ValueError):

    @property
    def title(self) -> str:
        return "Required MongoDB environment variables are missing"


@dataclass(frozen=True)
class MongoDBConfig:
    host: str
    port: int
    user: str
    password: str
    db_name: str
    collection_name: str

    @property
    def uri(self) -> str:
        return (
            f"mongodb://{self.user}:{self.password}@{self.host}"
            f":{self.port}/"
        )


def load_database_config() -> MongoDBConfig:
    host = environ.get("MONGO_HOST")
    port = environ.get("MONGO_PORT")
    user = environ.get("MONGO_INITDB_ROOT_USERNAME")
    password = environ.get("MONGO_INITDB_ROOT_PASSWORD")
    db_name = environ.get("MONGO_DB_NAME")
    collection_name = environ.get("MONGO_COLLECTION_NAME", "example")

    if (
            host is None
            or port is None
            or user is None
            or password is None
            or db_name is None
    ):
        raise MissingDatabaseConfigError

    return MongoDBConfig(
        host=host,
        port=int(port),
        user=user,
        password=password,
        db_name=db_name,
        collection_name=collection_name,
    )


@dataclass(frozen=True)
class Config:
    database: MongoDBConfig


def load_settings() -> Config:
    database = load_database_config()
    return Config(
        database=database,
    )
