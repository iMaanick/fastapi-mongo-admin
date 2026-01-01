import copy
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import Mock

import pytest

from app.example import Session, instrument_class


def reset_tracking_state(
    session: Session,
    instance: Any,
    field_name: str = "tags",
) -> None:
    """Сбрасывает состояние отслеживания изменений для тестирования.

    Args:
        session: Сессия отслеживания
        instance: Отслеживаемый объект
        field_name: Имя поля для сброса (по умолчанию "tags")
    """
    state = session._instance_states[id(instance)]  # noqa: SLF001
    state.changed_fields.clear()
    field_value = getattr(instance, field_name)
    state.original_values[field_name] = copy.deepcopy(field_value)


@dataclass
class Tag:
    title: str

    def __repr__(self) -> str:
        return f"Tag({self.title})"


@dataclass
class NestedTag:
    name: str
    level: int = 1

    def __repr__(self) -> str:
        return f"NestedTag({self.name}, level={self.level})"


@dataclass
class DeepTag:
    title: str
    nested: NestedTag | None = None

    def __repr__(self) -> str:
        return f"DeepTag({self.title}, nested={self.nested})"


@dataclass
class User:
    username: str
    email: str
    tags: list[Tag] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    _id: str | None = None


@dataclass
class ComplexUser:
    username: str
    tags: list[DeepTag] = field(default_factory=list)
    _id: str | None = None


instrument_class(Tag)
instrument_class(NestedTag)
instrument_class(DeepTag)
instrument_class(User)
instrument_class(ComplexUser)


@pytest.fixture
def mock_db() -> Mock:
    """Мок MongoDB базы данных"""
    db = Mock()
    db.__getitem__ = Mock(return_value=Mock())
    return db


@pytest.fixture
def session(mock_db: Mock) -> Any:
    """Создает новую сессию для каждого теста"""
    s = Session(
        db=mock_db,
        collection_mapping={
            Tag: "tags",
            User: "users",
            ComplexUser: "complex_users",
        },
    )
    yield s
    s.close()


@pytest.fixture
def simple_user() -> User:
    """Простой пользователь без списков"""
    return User(username="john", email="john@example.com")


@pytest.fixture
def user_with_tags() -> User:
    """Пользователь с тегами"""
    return User(
        username="alice",
        email="alice@example.com",
        tags=[Tag("python"), Tag("django")],
    )


@pytest.fixture
def complex_user() -> ComplexUser:
    """Пользователь с глубоко вложенными объектами"""
    return ComplexUser(
        username="bob",
        tags=[
            DeepTag("backend", NestedTag("api", 1)),
            DeepTag("frontend", NestedTag("ui", 2)),
        ],
    )


class TestBasicTracking:
    """Тесты базового отслеживания изменений"""

    def test_no_changes_initially(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Новый объект не должен иметь изменений"""
        session.add(simple_user)
        assert simple_user.get_changed_fields() == set()

    def test_track_single_field_change(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Отслеживание изменения одного поля"""
        session.add(simple_user)
        simple_user.username = "jane"

        assert simple_user.get_changed_fields() == {"username"}
        assert simple_user.get_original_value("username") == "john"

    def test_track_multiple_field_changes(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Отслеживание изменений нескольких полей"""
        session.add(simple_user)
        simple_user.username = "jane"
        simple_user.email = "jane@example.com"

        assert simple_user.get_changed_fields() == {"username", "email"}
        assert simple_user.get_original_value("username") == "john"
        original_email = "john@example.com"
        assert simple_user.get_original_value("email") == original_email

    def test_track_same_field_multiple_times(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Изменение одного поля несколько раз"""
        session.add(simple_user)
        original = simple_user.username

        simple_user.username = "jane"
        simple_user.username = "bob"
        simple_user.username = "alice"

        assert simple_user.get_changed_fields() == {"username"}
        assert simple_user.get_original_value("username") == original

    def test_untracked_object_has_no_changes(
        self,
        simple_user: User,
    ) -> None:
        """Объект не в сессии не отслеживается"""
        simple_user.username = "changed"
        assert simple_user.get_changed_fields() == set()
        assert simple_user.get_original_value("username") is None


class TestListTracking:
    """Тесты отслеживания изменений в списках"""

    def test_list_append(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Добавление элемента в список"""
        session.add(user_with_tags)
        user_with_tags.tags.append(Tag("flask"))

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.get_original_value("tags")) == 2
        assert len(user_with_tags.tags) == 3

    def test_list_remove(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Удаление элемента из списка"""
        session.add(user_with_tags)
        original_tags = user_with_tags.get_original_value("tags")

        user_with_tags.tags.pop(0)

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.tags) == 1
        assert len(original_tags) == 2

    def test_list_extend(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Расширение списка"""
        session.add(user_with_tags)
        user_with_tags.tags.extend([Tag("fastapi"), Tag("asyncio")])

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.tags) == 4

    def test_list_insert(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Вставка элемента в список"""
        session.add(user_with_tags)
        user_with_tags.tags.insert(0, Tag("first"))

        assert "tags" in user_with_tags.get_changed_fields()
        assert user_with_tags.tags[0].title == "first"

    def test_list_clear(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Очистка списка"""
        session.add(user_with_tags)
        user_with_tags.tags.clear()

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.tags) == 0
        assert len(user_with_tags.get_original_value("tags")) == 2

    def test_list_setitem(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Замена элемента по индексу"""
        session.add(user_with_tags)
        user_with_tags.tags[0] = Tag("replaced")

        assert "tags" in user_with_tags.get_changed_fields()
        assert user_with_tags.tags[0].title == "replaced"

    def test_list_delitem(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Удаление элемента по индексу"""
        session.add(user_with_tags)
        del user_with_tags.tags[0]

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.tags) == 1

    def test_empty_list_append(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Добавление в пустой список"""
        session.add(simple_user)
        simple_user.tags.append(Tag("first"))

        assert "tags" in simple_user.get_changed_fields()
        assert len(simple_user.tags) == 1


class TestNestedObjectTracking:
    """Тесты отслеживания изменений вложенных объектов"""

    def test_nested_object_field_change(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Изменение поля вложенного объекта в списке"""
        session.add(user_with_tags)
        user_with_tags.tags[0].title = "modified"

        assert "tags" in user_with_tags.get_changed_fields()
        original = user_with_tags.get_original_value("tags")
        assert original[0].title == "python"

    def test_deeply_nested_object_change(
        self,
        session: Any,
        complex_user: ComplexUser,
    ) -> None:
        """Изменение глубоко вложенного объекта"""
        session.add(complex_user)
        nested = complex_user.tags[0].nested
        if nested:
            nested.name = "modified_api"

        assert "tags" in complex_user.get_changed_fields()
        original = complex_user.get_original_value("tags")
        assert original[0].nested.name == "api"

    def test_multiple_nesting_levels(
        self,
        session: Any,
        complex_user: ComplexUser,
    ) -> None:
        """Изменения на разных уровнях вложенности"""
        session.add(complex_user)

        complex_user.tags[0].title = "new_backend"
        nested = complex_user.tags[0].nested
        if nested:
            nested.name = "new_api"
            nested.level = 99

        assert "tags" in complex_user.get_changed_fields()
        original = complex_user.get_original_value("tags")
        assert original[0].title == "backend"
        assert original[0].nested.name == "api"
        assert original[0].nested.level == 1


class TestEdgeCases:
    """Тесты граничных случаев"""

    def test_none_values(self, session: Any) -> None:
        """Работа с None значениями"""

        @dataclass
        class NullableUser:
            username: str | None = None
            tags: list[Any] | None = None

        instrument_class(NullableUser)

        user = NullableUser()
        session.collection_mapping[NullableUser] = "nullable_users"
        session.add(user)

        user.username = "john"
        assert "username" in user.get_changed_fields()
        assert user.get_original_value("username") is None

    def test_empty_strings(self, session: Any) -> None:
        """Работа с пустыми строками"""
        user = User(username="", email="")
        session.add(user)

        user.username = "john"
        assert user.get_original_value("username") == ""

    def test_special_characters(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Работа со специальными символами"""
        session.add(simple_user)
        simple_user.username = "user@#$%^&*()"
        simple_user.email = "test+special@example.com"

        assert simple_user.get_changed_fields() == {"username", "email"}

    def test_unicode_characters(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Работа с Unicode символами"""
        session.add(simple_user)
        simple_user.username = "用户名"
        simple_user.email = "тест@example.com"

        assert simple_user.get_changed_fields() == {"username", "email"}

    def test_very_long_strings(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Работа с очень длинными строками"""
        session.add(simple_user)
        long_string = "a" * 10000
        simple_user.username = long_string

        assert simple_user.get_original_value("username") == "john"
        assert len(simple_user.username) == 10000

    def test_circular_reference_in_list(self, session: Any) -> None:
        """Список с циклическими ссылками"""

        @dataclass
        class Node:
            name: str
            children: list[Any] = field(default_factory=list)

        instrument_class(Node)

        root = Node("root")
        child = Node("child")
        root.children.append(child)

        session.collection_mapping[Node] = "nodes"
        session.add(root)
        root.children.append(Node("new"))

        assert "children" in root.get_changed_fields()

    def test_large_list(self, session: Any, simple_user: User) -> None:
        """Работа с большим списком"""
        session.add(simple_user)
        large_list = [Tag(f"tag_{i}") for i in range(1000)]
        simple_user.tags = large_list

        assert "tags" in simple_user.get_changed_fields()

    def test_same_value_assignment(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Присвоение того же значения должно отслеживаться"""
        session.add(simple_user)
        original = simple_user.username
        simple_user.username = original

        assert "username" in simple_user.get_changed_fields()

    def test_boolean_fields(self, session: Any) -> None:
        """Работа с булевыми полями"""

        @dataclass
        class FlagUser:
            is_active: bool = False
            is_admin: bool = False

        instrument_class(FlagUser)

        user = FlagUser()
        session.collection_mapping[FlagUser] = "flag_users"
        session.add(user)

        user.is_active = True
        assert "is_active" in user.get_changed_fields()
        assert user.get_original_value("is_active") is False

    def test_numeric_fields(self, session: Any) -> None:
        """Работа с числовыми полями"""

        @dataclass
        class NumericUser:
            age: int = 0
            balance: float = 0.0

        instrument_class(NumericUser)

        user = NumericUser()
        session.collection_mapping[NumericUser] = "numeric_users"
        session.add(user)

        user.age = 25
        user.balance = 100.50

        assert user.get_changed_fields() == {"age", "balance"}
        assert user.get_original_value("age") == 0
        assert user.get_original_value("balance") == 0.0


class TestMultipleInstances:
    """Тесты с несколькими экземплярами"""

    def test_multiple_instances_independent(self, session: Any) -> None:
        """Изменения в разных экземплярах независимы"""
        user1 = User(username="alice", email="alice@example.com")
        user2 = User(username="bob", email="bob@example.com")

        session.add(user1)
        session.add(user2)

        user1.username = "alice_modified"

        assert "username" in user1.get_changed_fields()
        assert "username" not in user2.get_changed_fields()

    def test_same_class_multiple_instances(self, session: Any) -> None:
        """Несколько экземпляров одного класса"""
        users = [
            User(
                username=f"user{i}",
                email=f"user{i}@example.com",
            )
            for i in range(5)
        ]

        for user in users:
            session.add(user)

        users[0].username = "changed"
        users[2].email = "changed@example.com"

        assert "username" in users[0].get_changed_fields()
        assert "email" in users[2].get_changed_fields()
        assert users[1].get_changed_fields() == set()
        assert users[3].get_changed_fields() == set()


class TestMultipleSessions:
    """Тесты с несколькими сессиями"""

    def test_sessions_are_isolated(self, mock_db: Mock) -> None:
        """Две сессии должны быть изолированы друг от друга"""
        session1 = Session(
            db=mock_db,
            collection_mapping={User: "users"},
        )
        session2 = Session(
            db=mock_db,
            collection_mapping={User: "users"},
        )

        try:
            user1 = User(username="user1", email="user1@example.com")
            user2 = User(username="user2", email="user2@example.com")

            session1.add(user1)
            session2.add(user2)

            user1.username = "modified1"
            user2.username = "modified2"

            assert "username" in user1.get_changed_fields()
            assert "username" in user2.get_changed_fields()
            assert user1.get_original_value("username") == "user1"
            assert user2.get_original_value("username") == "user2"
        finally:
            session1.close()
            session2.close()

    def test_object_in_multiple_sessions(self, mock_db: Mock) -> None:
        """Один объект может быть добавлен в разные сессии"""
        session1 = Session(
            db=mock_db,
            collection_mapping={User: "users"},
        )
        session2 = Session(
            db=mock_db,
            collection_mapping={User: "users"},
        )

        try:
            user = User(username="shared", email="shared@example.com")

            session1.add(user)
            session2.add(user)

            user.username = "modified"

            assert "username" in user.get_changed_fields()
            assert user.get_original_value("username") == "shared"
        finally:
            session1.close()
            session2.close()


class TestBuildUpdateQuery:
    """Тесты генерации MongoDB запросов"""

    def test_simple_field_update(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Генерация запроса для простого изменения"""
        session.add(simple_user)
        simple_user.username = "jane"

        query = session.build_update_query(simple_user)
        assert query == {"$set": {"username": "jane"}}

    def test_multiple_fields_update(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Генерация запроса для нескольких полей"""
        session.add(simple_user)
        simple_user.username = "jane"
        simple_user.email = "jane@example.com"

        query = session.build_update_query(simple_user)
        assert "$set" in query
        assert query["$set"]["username"] == "jane"
        assert query["$set"]["email"] == "jane@example.com"

    def test_list_update(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Генерация запроса при изменении списка"""
        session.add(user_with_tags)
        user_with_tags.tags.append(Tag("new"))

        query = session.build_update_query(user_with_tags)
        assert "$set" in query
        assert "tags" in query["$set"]
        assert len(query["$set"]["tags"]) == 3
        assert query["$set"]["tags"][-1] == {"title": "new"}

    def test_nested_object_serialization(
        self,
        session: Any,
        complex_user: ComplexUser,
    ) -> None:
        """Сериализация вложенных объектов"""
        session.add(complex_user)
        nested = complex_user.tags[0].nested
        if nested:
            nested.name = "modified"

        query = session.build_update_query(complex_user)
        assert "$set" in query
        assert "tags" in query["$set"]

        first_tag = query["$set"]["tags"][0]
        assert first_tag["title"] == "backend"
        assert first_tag["nested"]["name"] == "modified"
        assert first_tag["nested"]["level"] == 1

    def test_no_changes_no_query(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Нет изменений - нет запроса"""
        session.add(simple_user)
        query = session.build_update_query(simple_user)
        assert query is None

    def test_id_field_excluded(self, session: Any) -> None:
        """Поле _id исключается из update"""
        user = User(username="john", email="john@example.com", _id="123")
        session.add(user)
        user._id = "456"  # noqa: SLF001
        user.username = "jane"

        query = session.build_update_query(user)
        assert "_id" not in query["$set"]
        assert "username" in query["$set"]


class TestTrackedObjectProxy:
    """Тесты TrackedObject прокси"""

    def test_tracked_object_repr(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """TrackedObject должен корректно работать с __repr__"""
        session.add(user_with_tags)
        tag = user_with_tags.tags[0]
        assert "Tag(python)" in repr(tag)

    def test_tracked_object_equality(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """TrackedObject должен поддерживать сравнение"""
        session.add(user_with_tags)
        tag1 = user_with_tags.tags[0]
        tag2 = Tag("python")

        assert tag1.title == tag2.title

    def test_tracked_object_attribute_access(
        self,
        session: Any,
        user_with_tags: User,
    ) -> None:
        """Доступ к атрибутам TrackedObject"""
        session.add(user_with_tags)
        tag = user_with_tags.tags[0]

        assert hasattr(tag, "title")
        assert tag.title == "python"


@dataclass
class ChangeTestCase:
    """Тестовый случай для проверки изменений"""

    id: str
    setup: Callable[[User], Any]
    expected_fields: set[str]
    description: str


CHANGE_TEST_CASES = [
    ChangeTestCase(
        id="single_field",
        setup=lambda u: setattr(u, "username", "new"),
        expected_fields={"username"},
        description="Изменение одного поля",
    ),
    ChangeTestCase(
        id="two_fields",
        setup=lambda u: [
            setattr(u, "username", "new"),
            setattr(u, "email", "new@example.com"),
        ],
        expected_fields={"username", "email"},
        description="Изменение двух полей",
    ),
    ChangeTestCase(
        id="list_modification",
        setup=lambda u: u.tags.append(Tag("new")),
        expected_fields={"tags"},
        description="Модификация списка",
    ),
]


@pytest.mark.parametrize(
    "test_case",
    [pytest.param(tc, id=tc.id) for tc in CHANGE_TEST_CASES],
)
def test_parametrized_changes(
    session: Any,
    user_with_tags: User,
    test_case: ChangeTestCase,
) -> None:
    """Параметризованный тест различных изменений"""
    session.add(user_with_tags)
    test_case.setup(user_with_tags)
    assert user_with_tags.get_changed_fields() == test_case.expected_fields


class TestPerformance:
    """Тесты производительности"""

    def test_many_field_changes(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Много изменений одного поля"""
        session.add(simple_user)

        for i in range(1000):
            simple_user.username = f"user_{i}"

        assert "username" in simple_user.get_changed_fields()
        assert simple_user.get_original_value("username") == "john"

    def test_many_list_operations(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Много операций со списком"""
        session.add(simple_user)

        for i in range(100):
            simple_user.tags.append(Tag(f"tag_{i}"))

        assert len(simple_user.tags) == 100
        assert len(simple_user.get_original_value("tags")) == 0


class TestNestedLists:
    """Тесты вложенных списков произвольной глубины"""

    def test_simple_nested_list(self, session: Any, simple_user: User) -> None:
        """Простой вложенный список"""
        session.add(simple_user)
        simple_user.tags = [Tag("python"), ["nested", "list"]]

        assert "tags" in simple_user.get_changed_fields()
        assert len(simple_user.tags) == 2
        assert simple_user.tags[1] == ["nested", "list"]

    def test_nested_list_append(self, session: Any, simple_user: User) -> None:
        """Добавление в вложенный список"""
        session.add(simple_user)
        simple_user.tags = [Tag("python"), ["nested"]]

        simple_user.get_changed_fields()

        simple_user.tags[1].append("new_item")

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[1] == ["nested", "new_item"]

    def test_deeply_nested_lists(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Глубоко вложенные списки (3 уровня)"""
        session.add(simple_user)
        simple_user.tags = [
            Tag("python"),
            ["level1", ["level2", ["level3"]]],
        ]

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[1][1][1] == ["level3"]

    def test_nested_list_modification_deep(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Изменение глубоко вложенного списка"""
        session.add(simple_user)
        simple_user.tags = [["level1", ["level2", ["level3"]]]]

        reset_tracking_state(session, simple_user)

        simple_user.tags[0][1][1].append("deep_item")

        assert "tags" in simple_user.get_changed_fields()
        assert "deep_item" in simple_user.tags[0][1][1]

    def test_nested_list_with_objects(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Вложенный список с объектами"""
        session.add(simple_user)
        simple_user.tags = [Tag("python"), ["nested", Tag("javascript")]]

        assert "tags" in simple_user.get_changed_fields()
        assert hasattr(simple_user.tags[0], "title")
        assert simple_user.tags[0].title == "python"
        assert isinstance(simple_user.tags[1], list)
        assert isinstance(simple_user.tags[1][1], Tag)

    def test_nested_list_object_modification(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Изменение объекта в вложенном списке"""
        session.add(simple_user)
        simple_user.tags = [Tag("python"), ["nested", Tag("js")]]

        reset_tracking_state(session, simple_user)

        simple_user.tags[1][1].title = "javascript"

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[1][1].title == "javascript"

    def test_multiple_nested_levels_mixed(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Смешанные типы на разных уровнях вложенности"""
        session.add(simple_user)
        simple_user.tags = [
            Tag("python"),
            ["string", Tag("js"), ["deep", Tag("ts")]],
        ]

        assert "tags" in simple_user.get_changed_fields()
        assert hasattr(simple_user.tags[0], "title")
        assert simple_user.tags[0].title == "python"
        assert isinstance(simple_user.tags[1][1], Tag)
        assert isinstance(simple_user.tags[1][2][1], Tag)

    def test_nested_list_clear(self, session: Any, simple_user: User) -> None:
        """Очистка вложенного списка"""
        session.add(simple_user)
        simple_user.tags = [Tag("python"), ["item1", "item2"]]

        reset_tracking_state(session, simple_user)

        simple_user.tags[1].clear()

        assert "tags" in simple_user.get_changed_fields()
        assert len(simple_user.tags[1]) == 0

    def test_nested_list_pop(self, session: Any, simple_user: User) -> None:
        """Pop из вложенного списка"""
        session.add(simple_user)
        simple_user.tags = [["item1", "item2", "item3"]]

        reset_tracking_state(session, simple_user)

        popped = simple_user.tags[0].pop()

        assert "tags" in simple_user.get_changed_fields()
        assert popped == "item3"
        assert len(simple_user.tags[0]) == 2

    def test_nested_list_remove(self, session: Any, simple_user: User) -> None:
        """Remove из вложенного списка"""
        session.add(simple_user)
        simple_user.tags = [["item1", "item2", "item3"]]

        reset_tracking_state(session, simple_user)

        simple_user.tags[0].remove("item2")

        assert "tags" in simple_user.get_changed_fields()
        assert "item2" not in simple_user.tags[0]

    def test_nested_list_insert(self, session: Any, simple_user: User) -> None:
        """Insert в вложенный список"""
        session.add(simple_user)
        simple_user.tags = [["item1", "item3"]]

        reset_tracking_state(session, simple_user)

        simple_user.tags[0].insert(1, "item2")

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[0][1] == "item2"

    def test_nested_list_extend(self, session: Any, simple_user: User) -> None:
        """Extend вложенного списка"""
        session.add(simple_user)
        simple_user.tags = [["item1"]]

        reset_tracking_state(session, simple_user)

        simple_user.tags[0].extend(["item2", "item3"])

        assert "tags" in simple_user.get_changed_fields()
        assert len(simple_user.tags[0]) == 3

    def test_nested_list_setitem(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Изменение элемента вложенного списка по индексу"""
        session.add(simple_user)
        simple_user.tags = [["old_value", "item2"]]

        reset_tracking_state(session, simple_user)

        simple_user.tags[0][0] = "new_value"

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[0][0] == "new_value"

    def test_nested_list_delitem(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Удаление элемента из вложенного списка по индексу"""
        session.add(simple_user)
        simple_user.tags = [["item1", "item2", "item3"]]

        reset_tracking_state(session, simple_user)

        del simple_user.tags[0][1]

        assert "tags" in simple_user.get_changed_fields()
        assert len(simple_user.tags[0]) == 2
        assert "item2" not in simple_user.tags[0]

    def test_very_deep_nesting(self, session: Any, simple_user: User) -> None:
        """Очень глубокая вложенность (5 уровней)"""
        session.add(simple_user)
        simple_user.tags = [
            [
                [
                    [
                        [
                            "level5",
                        ],
                    ],
                ],
            ],
        ]

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[0][0][0][0][0] == "level5"

    def test_nested_list_with_empty_lists(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Вложенные списки с пустыми списками"""
        session.add(simple_user)
        simple_user.tags = [[], ["item"], []]

        assert "tags" in simple_user.get_changed_fields()
        assert len(simple_user.tags[0]) == 0
        assert len(simple_user.tags[1]) == 1
        assert len(simple_user.tags[2]) == 0

    def test_nested_list_serialization(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Сериализация вложенных списков для MongoDB"""
        session.add(simple_user)
        simple_user.tags = [
            Tag("python"),
            ["string", Tag("js"), ["deep"]],
        ]

        query = session.build_update_query(simple_user)
        assert "$set" in query
        assert "tags" in query["$set"]

        tags_data = query["$set"]["tags"]
        assert tags_data[0] == {"title": "python"}
        assert tags_data[1][0] == "string"
        assert tags_data[1][1] == {"title": "js"}
        assert tags_data[1][2] == ["deep"]

    def test_nested_list_original_value_preserved(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Оригинальное значение вложенного списка сохраняется"""
        simple_user.tags = [["original", "values"]]
        session.add(simple_user)

        original = simple_user.get_original_value("tags")

        simple_user.tags[0].append("new")

        assert len(original[0]) == 2
        assert "new" not in original[0]

    def test_nested_object_with_nested_list_modification(
        self,
        session: Any,
    ) -> None:
        """Вложенный объект с вложенным списком"""

        @dataclass
        class Two:
            data: list[list[int]] = field(default_factory=list)

        @dataclass
        class One:
            username: str
            two: list[Two] = field(default_factory=list)
            _id: str | None = None

        instrument_class(Two)
        instrument_class(One)

        one = One(
            username="test",
            two=[
                Two(data=[[1, 2], [3, 4]]),
                Two(data=[[5, 6]]),
            ],
        )

        session.collection_mapping[One] = "ones"
        session.add(one)

        one.two[0].data[0].append(999)

        assert "two" in one.get_changed_fields()
        assert one.two[0].data[0][-1] == 999
        assert len(one.two[0].data[0]) == 3

        original = one.get_original_value("two")
        assert len(original[0].data[0]) == 2
        assert 999 not in original[0].data[0]

        query = session.build_update_query(one)
        assert query is not None
        assert "$set" in query
        assert "two" in query["$set"]

        serialized = query["$set"]["two"]
        assert serialized[0]["data"][0] == [1, 2, 999]

    def test_nested_object_field_modification(self, session: Any) -> None:
        """Изменение поля вложенного объекта"""

        @dataclass
        class Profile:
            bio: str
            age: int

        @dataclass
        class User:
            username: str
            profile: Profile
            _id: str | None = None

        instrument_class(Profile)
        instrument_class(User)

        user = User(
            username="test",
            profile=Profile(bio="Original bio", age=25),
        )

        session.collection_mapping[User] = "users"
        session.add(user)

        user.profile.bio = "New bio"

        msg = "Nested object field modification is not tracked!"
        assert "profile" in user.get_changed_fields(), msg

        assert user.profile.bio == "New bio"

        query = session.build_update_query(user)
        assert query is not None, "Update query should be generated"
        assert "profile" in query["$set"], "profile should be in $set"


class TestNestedListsEdgeCases:
    """Граничные случаи для вложенных списков"""

    def test_alternating_nesting(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Чередующаяся вложенность: объект -> список -> объект"""
        session.add(simple_user)
        simple_user.tags = [
            Tag("python"),
            [Tag("js"), [Tag("ts"), ["go"]]],
        ]

        assert "tags" in simple_user.get_changed_fields()
        assert hasattr(simple_user.tags[0], "title")
        assert simple_user.tags[0].title == "python"
        assert isinstance(simple_user.tags[1][0], Tag)
        assert isinstance(simple_user.tags[1][1][0], Tag)
        assert isinstance(simple_user.tags[1][1][1][0], str)

    def test_nested_list_with_none(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Вложенный список с None значениями"""
        session.add(simple_user)
        simple_user.tags = [None, ["item", None], [[None]]]

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[0] is None
        assert simple_user.tags[1][1] is None
        assert simple_user.tags[2][0][0] is None

    def test_nested_list_modification_preserves_structure(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Изменение вложенного списка сохраняет структуру"""
        session.add(simple_user)
        simple_user.tags = [
            Tag("a"),
            ["b", ["c", ["d"]]],
        ]

        reset_tracking_state(session, simple_user)

        simple_user.tags.append(Tag("e"))
        simple_user.tags[1].append("f")
        simple_user.tags[1][1].append("g")

        assert "tags" in simple_user.get_changed_fields()
        assert len(simple_user.tags) == 3
        assert len(simple_user.tags[1]) == 3
        assert len(simple_user.tags[1][1]) == 3

    def test_replacing_nested_list(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Замена вложенного списка целиком"""
        session.add(simple_user)
        simple_user.tags = [["old"]]

        reset_tracking_state(session, simple_user)

        simple_user.tags[0] = ["new", "list"]

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[0] == ["new", "list"]

    def test_nested_list_with_duplicates(
        self,
        session: Any,
        simple_user: User,
    ) -> None:
        """Вложенный список с дублирующимися элементами"""
        session.add(simple_user)
        tag = Tag("dup")
        simple_user.tags = [tag, [tag, [tag]]]

        assert "tags" in simple_user.get_changed_fields()
        assert simple_user.tags[0].title == "dup"
        assert simple_user.tags[1][0].title == "dup"
        assert simple_user.tags[1][1][0].title == "dup"

    def test_triple_nested_list_int_modification(self, session: Any) -> None:
        """Трёхуровневая вложенность списков с int"""

        @dataclass
        class DeepData:
            data: list[list[list[int]]] = field(default_factory=list)

        instrument_class(DeepData)

        deep = DeepData(data=[[[1, 2], [3, 4]], [[5, 6]]])
        session.collection_mapping[DeepData] = "deep_data"
        session.add(deep)

        deep.data[0][0].append(99)

        assert "data" in deep.get_changed_fields()
        assert deep.data[0][0][-1] == 99
        assert len(deep.data[0][0]) == 3

        original = deep.get_original_value("data")
        assert len(original[0][0]) == 2
        assert original[0][0] == [1, 2]

    def test_triple_nested_list_modify_second_level(
        self,
        session: Any,
    ) -> None:
        """Изменение на втором уровне трёхуровневой вложенности"""

        @dataclass
        class DeepData:
            data: list[list[list[int]]] = field(default_factory=list)

        instrument_class(DeepData)

        deep = DeepData(data=[[[1, 2]], [[3, 4]]])
        session.collection_mapping[DeepData] = "deep_data"
        session.add(deep)

        deep.data[0].append([99, 100])

        assert "data" in deep.get_changed_fields()
        assert deep.data[0][-1] == [99, 100]
        assert len(deep.data[0]) == 2

        original = deep.get_original_value("data")
        assert len(original[0]) == 1

    def test_triple_nested_list_modify_first_level(self, session: Any) -> None:
        """Изменение на первом уровне трёхуровневой вложенности"""

        @dataclass
        class DeepData:
            data: list[list[list[int]]] = field(default_factory=list)

        instrument_class(DeepData)

        deep = DeepData(data=[[[1, 2]]])
        session.collection_mapping[DeepData] = "deep_data"
        session.add(deep)

        deep.data.append([[99, 100], [101, 102]])

        assert "data" in deep.get_changed_fields()
        assert len(deep.data) == 2
        assert deep.data[1][0] == [99, 100]

        original = deep.get_original_value("data")
        assert len(original) == 1

    def test_triple_nested_list_serialization(self, session: Any) -> None:
        """Сериализация трёхуровневого списка"""

        @dataclass
        class DeepData:
            data: list[list[list[int]]] = field(default_factory=list)
            _id: str | None = None

        instrument_class(DeepData)

        deep = DeepData(data=[[[1, 2], [3]], [[4, 5, 6]]])
        session.collection_mapping[DeepData] = "deep_data"
        session.add(deep)

        deep.data[0][0].append(999)

        query = session.build_update_query(deep)
        assert query is not None
        assert "$set" in query

        serialized = query["$set"]["data"]
        assert serialized[0][0] == [1, 2, 999]
        assert serialized[0][1] == [3]
        assert serialized[1][0] == [4, 5, 6]


@dataclass
class NestedListTestCase:
    """Тестовый случай для вложенных списков"""

    id: str
    initial_value: list[Any]
    modification: Callable[[list[Any]], Any]
    expected_modified: bool
    description: str


NESTED_LIST_TEST_CASES = [
    NestedListTestCase(
        id="append_to_nested",
        initial_value=[["a", "b"]],
        modification=lambda lst: lst[0].append("c"),
        expected_modified=True,
        description="Append к вложенному списку",
    ),
    NestedListTestCase(
        id="modify_deep_nested",
        initial_value=[[["deep"]]],
        modification=lambda lst: lst[0][0].append("item"),
        expected_modified=True,
        description="Изменение глубоко вложенного списка",
    ),
    NestedListTestCase(
        id="replace_nested_item",
        initial_value=[["old"]],
        modification=lambda lst: lst[0].__setitem__(0, "new"),
        expected_modified=True,
        description="Замена элемента во вложенном списке",
    ),
]


@pytest.mark.parametrize(
    "test_case",
    [pytest.param(tc, id=tc.id) for tc in NESTED_LIST_TEST_CASES],
)
def test_parametrized_nested_lists(
    session: Any,
    simple_user: User,
    test_case: NestedListTestCase,
) -> None:
    """Параметризованный тест вложенных списков"""
    session.add(simple_user)
    simple_user.tags = test_case.initial_value

    reset_tracking_state(session, simple_user)

    test_case.modification(simple_user.tags)

    if test_case.expected_modified:
        assert "tags" in simple_user.get_changed_fields()
    else:
        assert "tags" not in simple_user.get_changed_fields()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
