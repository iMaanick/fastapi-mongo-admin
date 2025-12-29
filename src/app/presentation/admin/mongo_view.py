from collections.abc import Sequence
from typing import Any

from adaptix import Retort
from bson import ObjectId
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

    sortable_fields: tuple[str] = ("_id", "username", "email", "is_active")
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
        """Получение списка пользователей с фильтрацией и сортировкой"""
        container: AsyncContainer = request.state.dishka_container

        async with container() as req_container:
            repository: UserRepository = await req_container.get(
                UserRepository,
            )

            # Преобразуем where в MongoDB filter
            mongo_filter = build_mongo_filter(where)

            # Преобразуем order_by в MongoDB sort
            sort = None
            if order_by:
                sort = []
                for item in order_by:
                    key, direction = item.split(maxsplit=1)
                    sort_direction = -1 if direction.lower() == "desc" else 1
                    sort.append((key, sort_direction))

            # Получаем данные через repository
            return await repository.get_all(
                filter_query=mongo_filter,
                skip=skip,
                limit=max(0, limit),
                sort=sort,
            )

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
            repository: UserRepository = await req_container.get(
                UserRepository,
            )

            object_ids = []
            for pk in pks:
                try:
                    object_ids.append(ObjectId(str(pk)))
                except Exception:
                    continue

            if not object_ids:
                return []

            return await repository.get_all(
                filter_query={"_id": {"$in": object_ids}},
                skip=0,
                limit=0,
                sort=None,
            )

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        """Создание пользователя"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)
            change_tracker = await req_container.get(ChangeTracker)
            retort = await req_container.get(Retort)

            user = retort.load(data, User)

            await repository.add(user)

            await change_tracker.commit()

            return user

    async def edit(
        self,
        request: Request,
        pk: Any,
        data: dict[str, Any],
    ) -> Any:
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
            await change_tracker.commit()

            return updated_user

    async def delete(self, request: Request, pks: list[Any]) -> int | None:
        """Удаление пользователей"""
        container = request.state.dishka_container

        async with container() as req_container:
            repository = await req_container.get(UserRepository)
            change_tracker = await req_container.get(ChangeTracker)

            deleted = 0
            for pk in pks:
                try:
                    await repository.delete(str(pk))
                    deleted += 1
                except Exception:
                    continue
            await change_tracker.commit()
            return deleted
