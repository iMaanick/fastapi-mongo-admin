from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import Mock

import pytest

from app.example import Session


# ============= Тестовые модели =============


@dataclass
class Tag:
    title: str

    def __repr__(self):
        return f"Tag({self.title})"


@dataclass
class NestedTag:
    name: str
    level: int = 1

    def __repr__(self):
        return f"NestedTag({self.name}, level={self.level})"


@dataclass
class DeepTag:
    title: str
    nested: Optional[NestedTag] = None

    def __repr__(self):
        return f"DeepTag({self.title}, nested={self.nested})"


@dataclass
class User:
    username: str
    email: str
    tags: list[Tag] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    _id: Optional[str] = None


@dataclass
class ComplexUser:
    username: str
    tags: list[DeepTag] = field(default_factory=list)
    _id: Optional[str] = None


# ============= Фикстуры =============


@pytest.fixture
def mock_db():
    """Мок MongoDB базы данных"""
    db = Mock()
    # Мокаем коллекции
    db.__getitem__ = Mock(return_value=Mock())
    return db


@pytest.fixture
def session(mock_db):
    """Создает новую сессию для каждого теста"""
    s = Session(db=mock_db)
    yield s
    s.close()


@pytest.fixture
def simple_user():
    """Простой пользователь без списков"""
    return User(username="john", email="john@example.com")


@pytest.fixture
def user_with_tags():
    """Пользователь с тегами"""
    return User(
        username="alice", email="alice@example.com", tags=[Tag("python"), Tag("django")]
    )


@pytest.fixture
def complex_user():
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

    def test_no_changes_initially(self, session, simple_user):
        """Новый объект не должен иметь изменений"""
        session.add(simple_user)
        assert simple_user.get_changed_fields() == set()

    def test_track_single_field_change(self, session, simple_user):
        """Отслеживание изменения одного поля"""
        session.add(simple_user)
        simple_user.username = "jane"

        assert simple_user.get_changed_fields() == {"username"}
        assert simple_user.get_original_value("username") == "john"

    def test_track_multiple_field_changes(self, session, simple_user):
        """Отслеживание изменений нескольких полей"""
        session.add(simple_user)
        simple_user.username = "jane"
        simple_user.email = "jane@example.com"

        assert simple_user.get_changed_fields() == {"username", "email"}
        assert simple_user.get_original_value("username") == "john"
        assert simple_user.get_original_value("email") == "john@example.com"

    def test_track_same_field_multiple_times(self, session, simple_user):
        """Изменение одного поля несколько раз - сохраняется первое значение"""
        session.add(simple_user)
        original = simple_user.username

        simple_user.username = "jane"
        simple_user.username = "bob"
        simple_user.username = "alice"

        assert simple_user.get_changed_fields() == {"username"}
        assert simple_user.get_original_value("username") == original

    def test_untracked_object_has_no_changes(self, simple_user):
        """Объект не в сессии не отслеживается"""
        simple_user.username = "changed"
        assert simple_user.get_changed_fields() == set()
        assert simple_user.get_original_value("username") is None


class TestListTracking:
    """Тесты отслеживания изменений в списках"""

    def test_list_append(self, session, user_with_tags):
        """Добавление элемента в список"""
        session.add(user_with_tags)
        user_with_tags.tags.append(Tag("flask"))

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.get_original_value("tags")) == 2
        assert len(user_with_tags.tags) == 3

    def test_list_remove(self, session, user_with_tags):
        """Удаление элемента из списка"""
        session.add(user_with_tags)
        original_tags = user_with_tags.get_original_value("tags")

        user_with_tags.tags.pop(0)

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.tags) == 1
        # Оригинальное значение не должно измениться
        assert len(original_tags) == 2

    def test_list_extend(self, session, user_with_tags):
        """Расширение списка"""
        session.add(user_with_tags)
        user_with_tags.tags.extend([Tag("fastapi"), Tag("asyncio")])

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.tags) == 4

    def test_list_insert(self, session, user_with_tags):
        """Вставка элемента в список"""
        session.add(user_with_tags)
        user_with_tags.tags.insert(0, Tag("first"))

        assert "tags" in user_with_tags.get_changed_fields()
        assert user_with_tags.tags[0].title == "first"

    def test_list_clear(self, session, user_with_tags):
        """Очистка списка"""
        session.add(user_with_tags)
        user_with_tags.tags.clear()

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.tags) == 0
        assert len(user_with_tags.get_original_value("tags")) == 2

    def test_list_setitem(self, session, user_with_tags):
        """Замена элемента по индексу"""
        session.add(user_with_tags)
        user_with_tags.tags[0] = Tag("replaced")

        assert "tags" in user_with_tags.get_changed_fields()
        assert user_with_tags.tags[0].title == "replaced"

    def test_list_delitem(self, session, user_with_tags):
        """Удаление элемента по индексу"""
        session.add(user_with_tags)
        del user_with_tags.tags[0]

        assert "tags" in user_with_tags.get_changed_fields()
        assert len(user_with_tags.tags) == 1

    def test_empty_list_append(self, session, simple_user):
        """Добавление в пустой список"""
        session.add(simple_user)
        simple_user.tags.append(Tag("first"))

        assert "tags" in simple_user.get_changed_fields()
        assert len(simple_user.tags) == 1


class TestNestedObjectTracking:
    """Тесты отслеживания изменений вложенных объектов"""

    def test_nested_object_field_change(self, session, user_with_tags):
        """Изменение поля вложенного объекта в списке"""
        session.add(user_with_tags)
        user_with_tags.tags[0].title = "modified"

        assert "tags" in user_with_tags.get_changed_fields()
        original = user_with_tags.get_original_value("tags")
        assert original[0].title == "python"

    def test_deeply_nested_object_change(self, session, complex_user):
        """Изменение глубоко вложенного объекта"""
        session.add(complex_user)
        complex_user.tags[0].nested.name = "modified_api"

        assert "tags" in complex_user.get_changed_fields()
        original = complex_user.get_original_value("tags")
        assert original[0].nested.name == "api"

    def test_multiple_nesting_levels(self, session, complex_user):
        """Изменения на разных уровнях вложенности"""
        session.add(complex_user)

        # Меняем на разных уровнях
        complex_user.tags[0].title = "new_backend"
        complex_user.tags[0].nested.name = "new_api"
        complex_user.tags[0].nested.level = 99

        assert "tags" in complex_user.get_changed_fields()
        original = complex_user.get_original_value("tags")
        assert original[0].title == "backend"
        assert original[0].nested.name == "api"
        assert original[0].nested.level == 1


class TestEdgeCases:
    """Тесты граничных случаев"""

    def test_none_values(self, session):
        """Работа с None значениями"""

        @dataclass
        class NullableUser:
            username: Optional[str] = None
            tags: Optional[list] = None

        user = NullableUser()
        session.add(user)

        user.username = "john"
        assert "username" in user.get_changed_fields()
        assert user.get_original_value("username") is None

    def test_empty_strings(self, session):
        """Работа с пустыми строками"""
        user = User(username="", email="")
        session.add(user)

        user.username = "john"
        assert user.get_original_value("username") == ""

    def test_special_characters(self, session, simple_user):
        """Работа со специальными символами"""
        session.add(simple_user)
        simple_user.username = "user@#$%^&*()"
        simple_user.email = "test+special@example.com"

        assert simple_user.get_changed_fields() == {"username", "email"}

    def test_unicode_characters(self, session, simple_user):
        """Работа с Unicode символами"""
        session.add(simple_user)
        simple_user.username = "用户名"
        simple_user.email = "тест@example.com"

        assert simple_user.get_changed_fields() == {"username", "email"}

    def test_very_long_strings(self, session, simple_user):
        """Работа с очень длинными строками"""
        session.add(simple_user)
        long_string = "a" * 10000
        simple_user.username = long_string

        assert simple_user.get_original_value("username") == "john"
        assert len(simple_user.username) == 10000

    def test_circular_reference_in_list(self, session):
        """Список с циклическими ссылками"""

        @dataclass
        class Node:
            name: str
            children: list = field(default_factory=list)

        root = Node("root")
        child = Node("child")
        root.children.append(child)

        session.add(root)
        root.children.append(Node("new"))

        assert "children" in root.get_changed_fields()

    def test_large_list(self, session, simple_user):
        """Работа с большим списком"""
        session.add(simple_user)
        large_list = [Tag(f"tag_{i}") for i in range(1000)]
        simple_user.tags = large_list

        assert "tags" in simple_user.get_changed_fields()

    def test_same_value_assignment(self, session, simple_user):
        """Присвоение того же значения должно отслеживаться"""
        session.add(simple_user)
        original = simple_user.username
        simple_user.username = original

        # Всё равно считается изменением
        assert "username" in simple_user.get_changed_fields()

    def test_boolean_fields(self, session):
        """Работа с булевыми полями"""

        @dataclass
        class FlagUser:
            is_active: bool = False
            is_admin: bool = False

        user = FlagUser()
        session.add(user)

        user.is_active = True
        assert "is_active" in user.get_changed_fields()
        assert user.get_original_value("is_active") is False

    def test_numeric_fields(self, session):
        """Работа с числовыми полями"""

        @dataclass
        class NumericUser:
            age: int = 0
            balance: float = 0.0

        user = NumericUser()
        session.add(user)

        user.age = 25
        user.balance = 100.50

        assert user.get_changed_fields() == {"age", "balance"}
        assert user.get_original_value("age") == 0
        assert user.get_original_value("balance") == 0.0


class TestMultipleInstances:
    """Тесты с несколькими экземплярами"""

    def test_multiple_instances_independent(self, session):
        """Изменения в разных экземплярах независимы"""
        user1 = User(username="alice", email="alice@example.com")
        user2 = User(username="bob", email="bob@example.com")

        session.add(user1)
        session.add(user2)

        user1.username = "alice_modified"

        assert "username" in user1.get_changed_fields()
        assert "username" not in user2.get_changed_fields()

    def test_same_class_multiple_instances(self, session):
        """Несколько экземпляров одного класса"""
        users = [
            User(username=f"user{i}", email=f"user{i}@example.com") for i in range(5)
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

    def test_sessions_are_isolated(self, mock_db):
        """Две сессии должны быть изолированы друг от друга"""
        session1 = Session(db=mock_db)
        session2 = Session(db=mock_db)

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

    def test_object_in_multiple_sessions(self, mock_db):
        """Один объект может быть добавлен в разные сессии"""
        session1 = Session(db=mock_db)
        session2 = Session(db=mock_db)

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

    def test_simple_field_update(self, session, simple_user):
        """Генерация запроса для простого изменения"""
        session.add(simple_user)
        simple_user.username = "jane"

        query = session.build_update_query(simple_user)
        assert query == {"$set": {"username": "jane"}}

    def test_multiple_fields_update(self, session, simple_user):
        """Генерация запроса для нескольких полей"""
        session.add(simple_user)
        simple_user.username = "jane"
        simple_user.email = "jane@example.com"

        query = session.build_update_query(simple_user)
        assert "$set" in query
        assert query["$set"]["username"] == "jane"
        assert query["$set"]["email"] == "jane@example.com"

    def test_list_update(self, session, user_with_tags):
        """Генерация запроса при изменении списка"""
        session.add(user_with_tags)
        user_with_tags.tags.append(Tag("new"))

        query = session.build_update_query(user_with_tags)
        assert "$set" in query
        assert "tags" in query["$set"]
        assert len(query["$set"]["tags"]) == 3
        assert query["$set"]["tags"][-1] == {"title": "new"}

    def test_nested_object_serialization(self, session, complex_user):
        """Сериализация вложенных объектов"""
        session.add(complex_user)
        complex_user.tags[0].nested.name = "modified"

        query = session.build_update_query(complex_user)
        assert "$set" in query
        assert "tags" in query["$set"]

        # Проверяем структуру
        first_tag = query["$set"]["tags"][0]
        assert first_tag["title"] == "backend"
        assert first_tag["nested"]["name"] == "modified"
        assert first_tag["nested"]["level"] == 1

    def test_no_changes_no_query(self, session, simple_user):
        """Нет изменений - нет запроса"""
        session.add(simple_user)
        query = session.build_update_query(simple_user)
        assert query is None

    def test_id_field_excluded(self, session):
        """Поле _id исключается из update"""
        user = User(username="john", email="john@example.com", _id="123")
        session.add(user)
        user._id = "456"  # Не должно попасть в запрос
        user.username = "jane"

        query = session.build_update_query(user)
        assert "_id" not in query["$set"]
        assert "username" in query["$set"]


class TestTrackedObjectProxy:
    """Тесты TrackedObject прокси"""

    def test_tracked_object_repr(self, session, user_with_tags):
        """TrackedObject должен корректно работать с __repr__"""
        session.add(user_with_tags)
        tag = user_with_tags.tags[0]
        assert "Tag(python)" in repr(tag)

    def test_tracked_object_equality(self, session, user_with_tags):
        """TrackedObject должен поддерживать сравнение"""
        session.add(user_with_tags)
        tag1 = user_with_tags.tags[0]
        tag2 = Tag("python")

        # Должны быть равны по содержимому
        assert tag1.title == tag2.title

    def test_tracked_object_attribute_access(self, session, user_with_tags):
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
    setup: callable
    expected_fields: set
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
    "test_case", [pytest.param(tc, id=tc.id) for tc in CHANGE_TEST_CASES]
)
def test_parametrized_changes(session, user_with_tags, test_case: ChangeTestCase):
    """Параметризованный тест различных изменений"""
    session.add(user_with_tags)
    test_case.setup(user_with_tags)
    assert user_with_tags.get_changed_fields() == test_case.expected_fields


# ============= Тесты производительности =============


class TestPerformance:
    """Тесты производительности (не строгие, для выявления проблем)"""

    def test_many_field_changes(self, session, simple_user):
        """Много изменений одного поля"""
        session.add(simple_user)

        for i in range(1000):
            simple_user.username = f"user_{i}"

        assert "username" in simple_user.get_changed_fields()
        assert simple_user.get_original_value("username") == "john"

    def test_many_list_operations(self, session, simple_user):
        """Много операций со списком"""
        session.add(simple_user)

        for i in range(100):
            simple_user.tags.append(Tag(f"tag_{i}"))

        assert len(simple_user.tags) == 100
        assert len(simple_user.get_original_value("tags")) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
