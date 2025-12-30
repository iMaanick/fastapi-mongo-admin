from collections.abc import Sequence
from typing import Any

from dishka import AsyncContainer
from starlette.requests import Request
from starlette_admin import (
    BaseField,
    BaseModelView,
    BooleanField,
    CollectionField,
    ListField,
    StringField,
)

from app.application.interactors.admin.count_users import (
    CountUsersAdminInteractor,
    CountUsersAdminRequest,
)
from app.application.interactors.admin.create_user_admin import (
    CreateUserAdminInteractor,
    CreateUserAdminRequest,
)
from app.application.interactors.admin.delete_users_admin import (
    DeleteUsersAdminInteractor,
    DeleteUsersAdminRequest,
)
from app.application.interactors.admin.get_user_by_id import (
    GetUserByIdAdminInteractor,
    GetUserByIdAdminRequest,
)
from app.application.interactors.admin.get_users import (
    GetUsersAdminInteractor,
    GetUsersAdminRequest,
)
from app.application.interactors.admin.get_users_by_ids import (
    GetUsersByIdsAdminInteractor,
    GetUsersByIdsAdminRequest,
)
from app.application.interactors.admin.update_user_admin import (
    UpdateUserAdminInteractor,
    UpdateUserAdminRequest,
)
from app.infrastructure.db.query_builder import build_mongo_filter


class MongoUserView(BaseModelView):
    """Кастомная админка для MongoDB User"""

    identity = "user"
    name = "User"
    label = "Users"
    icon = "fa fa-users"
    pk_attr = "_id"

    fields: Sequence[BaseField] = [
        StringField("_id", exclude_from_create=True, exclude_from_edit=True),
        StringField("username", required=True),
        StringField("email", required=True),
        BooleanField("is_active"),
        ListField(
            CollectionField(
                "tags",
                fields=[
                    StringField("name", required=True),
                ],
            ),
        ),
    ]

    sortable_fields: tuple[str, str, str, str] = (
        "_id",
        "username",
        "email",
        "is_active",
    )
    searchable_fields: Sequence[str] = ["username", "email"]
    exclude_fields_from_list: Sequence[str] = ["unused_field"]
    fields_default_sort: Sequence[str] = ["_id"]
    page_size: int = 10
    page_size_options: Sequence[int] = [10, 25, 50, -1]

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: dict[str, Any] | str | None = None,
        order_by: list[str] | None = None,
    ) -> list[Any]:
        container: AsyncContainer = request.state.dishka_container

        async with container() as req_container:
            interactor: GetUsersAdminInteractor = await req_container.get(
                GetUsersAdminInteractor,
            )

            mongo_filter = build_mongo_filter(where)
            sort = self._build_sort(order_by)

            return await interactor(
                GetUsersAdminRequest(
                    filter_query=mongo_filter,
                    skip=skip,
                    limit=limit,
                    sort=sort,
                ),
            )

    async def count(
        self,
        request: Request,
        where: dict[str, Any] | str | None = None,
    ) -> int:
        container = request.state.dishka_container

        async with container() as req_container:
            interactor: CountUsersAdminInteractor = await req_container.get(
                CountUsersAdminInteractor,
            )

            mongo_filter = build_mongo_filter(where)

            return await interactor(
                CountUsersAdminRequest(filter_query=mongo_filter),
            )

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        container = request.state.dishka_container

        async with container() as req_container:
            interactor: GetUserByIdAdminInteractor = await req_container.get(
                GetUserByIdAdminInteractor,
            )

            return await interactor(GetUserByIdAdminRequest(user_id=str(pk)))

    async def find_by_pks(self, request: Request, pks: list[Any]) -> list[Any]:
        container = request.state.dishka_container

        async with container() as req_container:
            interactor: GetUsersByIdsAdminInteractor = await req_container.get(
                GetUsersByIdsAdminInteractor,
            )

            user_ids = [str(pk) for pk in pks]

            return await interactor(
                GetUsersByIdsAdminRequest(user_ids=user_ids),
            )

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        container = request.state.dishka_container

        async with container() as req_container:
            interactor: CreateUserAdminInteractor = await req_container.get(
                CreateUserAdminInteractor,
            )

            return await interactor(CreateUserAdminRequest(data=data))

    async def edit(
        self,
        request: Request,
        pk: Any,
        data: dict[str, Any],
    ) -> Any:
        container = request.state.dishka_container

        async with container() as req_container:
            interactor: UpdateUserAdminInteractor = await req_container.get(
                UpdateUserAdminInteractor,
            )

            return await interactor(
                UpdateUserAdminRequest(user_id=str(pk), data=data),
            )

    async def delete(self, request: Request, pks: list[Any]) -> int | None:
        container = request.state.dishka_container

        async with container() as req_container:
            interactor: DeleteUsersAdminInteractor = await req_container.get(
                DeleteUsersAdminInteractor,
            )

            user_ids = [str(pk) for pk in pks]

            return await interactor(
                DeleteUsersAdminRequest(user_ids=user_ids),
            )

    @staticmethod
    def _build_sort(
        order_by: list[str] | None,
    ) -> list[tuple[str, int]] | None:
        """Построить sort для MongoDB из order_by"""
        if not order_by:
            return None

        sort = []
        for item in order_by:
            key, direction = item.split(maxsplit=1)
            sort_direction = -1 if direction.lower() == "desc" else 1
            sort.append((key, sort_direction))

        return sort
