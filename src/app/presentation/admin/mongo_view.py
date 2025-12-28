from typing import Any

from adaptix import Retort
from bson import ObjectId
from starlette.requests import Request
from starlette_admin import (
    BaseModelView,
    BooleanField,
    CollectionField,
    ListField,
    StringField,
)

from app.application.change_tracker import ChangeTracker
from app.application.user_repo import UserRepository
from app.domain.model import User
from app.infrastructure.db.query_builder import build_mongo_filter


# по-хорошему в методах нужно дергать интеракторы, но мне лень (пока что)
class MongoUserView(BaseModelView):
    """Кастомная админка для MongoDB User"""

    identity = "user"
    name = "User"
    label = "Users"
    icon = "fa fa-users"
    pk_attr = "_id"

    fields = [
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

    sortable_fields = ("_id", "username", "email", "is_active")
    searchable_fields = ["username", "email"]
    exclude_fields_from_list = ["unused_field"]
    fields_default_sort = ["_id"]
    page_size = 10
    page_size_options = [10, 25, 50, -1]

    async def find_all(
            self,
            request: Request,
            skip: int = 0,
            limit: int = 100,
            where: dict[str, Any] | str | None = None,
            order_by: list[str] | None = None,
    ) -> list[Any]:
        """Получение списка пользователей с фильтрацией и сортировкой"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)

            # Преобразуем where в MongoDB filter
            mongo_filter = build_mongo_filter(where)

            # Преобразуем order_by в MongoDB sort
            sort = None
            if order_by:
                sort = []
                for item in order_by:
                    key, direction = item.split(maxsplit=1)
                    # MongoDB: 1 для ascending, -1 для descending
                    sort.append((key, -1 if direction.lower() == "desc" else 1))

            # Получаем данные через repository
            users = await repository.get_all(
                filter_query=mongo_filter,
                skip=skip,
                limit=limit if limit > 0 else 0,
                sort=sort,
            )

            return users

    async def count(
            self,
            request: Request,
            where: dict[str, Any] | str | None = None,
    ) -> int:
        """Подсчет количества документов с учётом фильтра"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)

            # Преобразуем where в MongoDB filter
            mongo_filter = build_mongo_filter(where)

            # Получаем все данные с фильтром (без пагинации)
            users = await repository.get_all(
                filter_query=mongo_filter,
                skip=0,
                limit=0,  # 0 = без лимита
                sort=None,
            )

            return len(users)

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        """Получение пользователя по ID"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)
            return await repository.get_by_id(str(pk))

    async def find_by_pks(self, request: Request, pks: list[Any]) -> list[Any]:
        """Получение пользователей по списку ID"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)

            object_ids = []
            for pk in pks:
                try:
                    object_ids.append(ObjectId(str(pk)))
                except Exception:
                    continue

            if not object_ids:
                return []

            users = await repository.get_all(
                filter_query={"_id": {"$in": object_ids}},
                skip=0,
                limit=0,
                sort=None,
            )

            return users

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        """Создание пользователя"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)
            retort = await req_container.get(Retort)

            user = retort.load(data, User)

            await repository.add(user)

            return user

    async def edit(self, request: Request, pk: Any, data: dict[str, Any]) -> Any:
        """Редактирование пользователя через ChangeTracker"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)
            change_tracker = await req_container.get(ChangeTracker)
            retort = await req_container.get(Retort)

            existing_user = await repository.get_by_id(str(pk))
            if not existing_user:
                raise ValueError(f"User {pk} not found")

            data["_id"] = str(pk)

            updated_user = retort.load(data, User)

            change_tracker.track(updated_user)

            await change_tracker.save()

            return updated_user

    async def delete(self, request: Request, pks: list[Any]) -> int | None:
        """Удаление пользователей"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)

            deleted = 0
            for pk in pks:
                try:
                    await repository.delete(str(pk))
                    deleted += 1
                except Exception:
                    continue

            return deleted
