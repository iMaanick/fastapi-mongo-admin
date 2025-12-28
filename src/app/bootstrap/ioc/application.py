from dishka import Provider, Scope, WithParents, provide_all

from app.application.interactors.create_user import CreateUserInteractor
from app.application.interactors.get_user import GetUserInteractor
from app.application.interactors.update_user import UpdateUserInteractor
from app.application.interactors.update_users import UpdateUsersInteractor
from app.infrastructure.db.user_repo import MongoUserRepository


class ApplicationProvider(Provider):
    interactors = provide_all(
        CreateUserInteractor,
        GetUserInteractor,
        UpdateUsersInteractor,
        UpdateUserInteractor,
        scope=Scope.REQUEST,
    )

    repos = provide_all(
        WithParents[MongoUserRepository],
        scope=Scope.REQUEST,
    )
