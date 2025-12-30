from dishka import Provider, Scope, WithParents, provide_all

from app.application.interactors.admin.count_users import CountUsersAdminInteractor
from app.application.interactors.admin.create_user_admin import CreateUserAdminInteractor
from app.application.interactors.admin.delete_users_admin import DeleteUsersAdminInteractor
from app.application.interactors.admin.get_user_by_id import GetUserByIdAdminInteractor
from app.application.interactors.admin.get_users import GetUsersAdminInteractor
from app.application.interactors.admin.get_users_by_ids import GetUsersByIdsAdminInteractor
from app.application.interactors.admin.update_user_admin import UpdateUserAdminInteractor
from app.application.interactors.user.create_user import CreateUserInteractor
from app.application.interactors.user.get_user import GetUserInteractor
from app.application.interactors.user.update_user import UpdateUserInteractor
from app.application.interactors.user.update_users import UpdateUsersInteractor
from app.infrastructure.db.user_repo import MongoUserRepository


class ApplicationProvider(Provider):
    interactors = provide_all(
        CreateUserInteractor,
        GetUserInteractor,
        UpdateUsersInteractor,
        UpdateUserInteractor,
        CountUsersAdminInteractor,
        CreateUserAdminInteractor,
        DeleteUsersAdminInteractor,
        GetUserByIdAdminInteractor,
        GetUsersAdminInteractor,
        GetUsersByIdsAdminInteractor,
        UpdateUserAdminInteractor,
        scope=Scope.REQUEST,
    )

    repos = provide_all(
        WithParents[MongoUserRepository],
        scope=Scope.REQUEST,
    )
