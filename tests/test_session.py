from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import Mock

import pytest

from app.example import Session, instrument_class

# ============= Тестовые модели =============


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


# Инструментируем классы один раз при импорте модуля
instrument_class(Tag)
instrument_class(NestedTag)
instrument_class(DeepTag)
instrument_class(User)
instrument_class(ComplexUser)


# ============= Фикстуры =============


@pytest.fixture
def mock_db() -> Mock:
    """Мок MongoDB базы данных"""
    db = Mock()
    # Мокаем коллекции
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


# ============= Тесты базовой функциональности =============


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
        assert simple_user.get_original_value("email") == "john@example.com"

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
        # Оригинальное значение не должно измениться
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
        complex_user.tags[0].nested.name = "modified_api"  # type: ignore[union-attr]

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

        # Меняем на разных уровнях
        complex_user.tags[0].title = "new_backend"
        complex_user.tags[0].nested.name = "new_api"  # type: ignore[union-attr]
        complex_user.tags[0].nested.level = 99  # type: ignore[union-attr]

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

        # Всё равно считается изменением
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

        # Изменяем только некоторых
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

            # Каждая сессия видит только свои изменения
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

            # Добавляем один объект в обе сессии
            session1.add(user)
            session2.add(user)

            # Изменяем - должны отслеживаться в обеих
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
        complex_user.tags[0].nested.name = "modified"  # type: ignore[union-attr]

        query = session.build_update_query(complex_user)
        assert "$set" in query
        assert "tags" in query["$set"]

        # Проверяем структуру
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
        user._id = "456"
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

        # Должны быть равны по содержимому
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


# ============= Параметризованные тесты =============


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


# ============= Тесты производительности =============


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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
