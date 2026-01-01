from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from adaptix import Retort
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClientSession, AsyncIOMotorDatabase

from app.application.change_tracker import (
    CollectionMappingNotFoundError,
    EntityMissingIdError,
    EntityNotDataclassError,
    InvalidEntityIdError,
)
from app.infrastructure.trackers.mongo_session import MongoSession

# ============= Test Models =============

@dataclass
class User:
    _id: str | None = None
    name: str = ""
    email: str = ""
    age: int = 0
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class Product:
    _id: str | None = None
    title: str = ""
    price: float = 0.0
    in_stock: bool = True


@dataclass
class Order:
    _id: str | None = None
    user_id: str = ""
    items: list[dict[str, Any]] = field(default_factory=list)
    total: float = 0.0


# Non-dataclass for error testing
class NotADataclass:
    def __init__(self):
        self.name = "test"


# ============= Fixtures =============

@pytest.fixture
def retort():
    """Create Retort instance"""
    return Retort()


@pytest.fixture
def mock_database():
    """Mock Motor database"""
    db = MagicMock(spec=AsyncIOMotorDatabase)
    return db


@pytest.fixture
def mock_session():
    """Mock Motor session"""
    session = AsyncMock(spec=AsyncIOMotorClientSession)
    session.in_transaction = True
    session.commit_transaction = AsyncMock()
    session.abort_transaction = AsyncMock()
    return session


@pytest.fixture
def collection_mapping():
    """Collection mapping for test entities"""
    return {
        User: "users",
        Product: "products",
        Order: "orders",
    }


@pytest.fixture
def mongo_session(mock_database, mock_session, retort, collection_mapping):
    """Create MongoSession instance"""
    return MongoSession(
        collection_mapping=collection_mapping,
        database=mock_database,
        retort=retort,
        session=mock_session,
    )


@pytest.fixture
def valid_object_id():
    """Generate valid ObjectId for tests"""

    def _generate():
        return str(ObjectId())

    return _generate


# ============= Tests: _get_changed_fields =============

def test_get_changed_fields_no_changes(mongo_session):
    """Test no changes detected"""
    original = {"name": "Alice", "age": 25, "tags": ["python"]}
    current = {"name": "Alice", "age": 25, "tags": ["python"]}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {}


def test_get_changed_fields_simple_field_changed(mongo_session):
    """Test simple field change"""
    original = {"name": "Alice", "age": 25}
    current = {"name": "Alice", "age": 26}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"age": 26}


def test_get_changed_fields_multiple_fields_changed(mongo_session):
    """Test multiple fields changed"""
    original = {"name": "Alice", "age": 25, "email": "old@test.com"}
    current = {"name": "Bob", "age": 30, "email": "new@test.com"}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"name": "Bob", "age": 30, "email": "new@test.com"}


def test_get_changed_fields_list_changed(mongo_session):
    """Test list field change - entire list replaced"""
    original = {"name": "Alice", "tags": ["python", "fastapi"]}
    current = {"name": "Alice", "tags": ["python", "django"]}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"tags": ["python", "django"]}


def test_get_changed_fields_list_order_changed(mongo_session):
    """Test list order change - should detect difference"""
    original = {"tags": ["python", "fastapi"]}
    current = {"tags": ["fastapi", "python"]}

    changes = mongo_session._get_changed_fields(original, current)

    # Order matters in lists
    assert changes == {"tags": ["fastapi", "python"]}


def test_get_changed_fields_list_item_added(mongo_session):
    """Test item added to list"""
    original = {"tags": ["python"]}
    current = {"tags": ["python", "fastapi"]}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"tags": ["python", "fastapi"]}


def test_get_changed_fields_list_item_removed(mongo_session):
    """Test item removed from list"""
    original = {"tags": ["python", "fastapi", "django"]}
    current = {"tags": ["python"]}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"tags": ["python"]}


def test_get_changed_fields_empty_list_to_populated(mongo_session):
    """Test empty list becomes populated"""
    original = {"tags": []}
    current = {"tags": ["python"]}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"tags": ["python"]}


def test_get_changed_fields_populated_list_to_empty(mongo_session):
    """Test populated list becomes empty"""
    original = {"tags": ["python", "fastapi"]}
    current = {"tags": []}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"tags": []}


def test_get_changed_fields_dict_changed(mongo_session):
    """Test dict field change - entire dict replaced"""
    original = {"metadata": {"role": "admin", "level": 5}}
    current = {"metadata": {"role": "user", "level": 5}}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"metadata": {"role": "user", "level": 5}}


def test_get_changed_fields_dict_key_added(mongo_session):
    """Test key added to dict"""
    original = {"metadata": {"role": "admin"}}
    current = {"metadata": {"role": "admin", "level": 5}}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"metadata": {"role": "admin", "level": 5}}


def test_get_changed_fields_dict_key_removed(mongo_session):
    """Test key removed from dict"""
    original = {"metadata": {"role": "admin", "level": 5}}
    current = {"metadata": {"role": "admin"}}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"metadata": {"role": "admin"}}


def test_get_changed_fields_empty_dict_to_populated(mongo_session):
    """Test empty dict becomes populated"""
    original = {"metadata": {}}
    current = {"metadata": {"role": "admin"}}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"metadata": {"role": "admin"}}


def test_get_changed_fields_nested_dict_changed(mongo_session):
    """Test nested dict change"""
    original = {"metadata": {"user": {"role": "admin"}}}
    current = {"metadata": {"user": {"role": "user"}}}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"metadata": {"user": {"role": "user"}}}


def test_get_changed_fields_field_added(mongo_session):
    """Test new field added"""
    original = {"name": "Alice"}
    current = {"name": "Alice", "email": "alice@test.com"}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"email": "alice@test.com"}


def test_get_changed_fields_field_removed(mongo_session):
    """Test field removed (becomes None)"""
    original = {"name": "Alice", "email": "alice@test.com"}
    current = {"name": "Alice"}

    changes = mongo_session._get_changed_fields(original, current)

    # Field not in current = None
    assert "email" in changes
    assert changes["email"] is None


def test_get_changed_fields_value_becomes_none(mongo_session):
    """Test value explicitly set to None"""
    original = {"name": "Alice", "email": "alice@test.com"}
    current = {"name": "Alice", "email": None}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"email": None}


def test_get_changed_fields_none_becomes_value(mongo_session):
    """Test None becomes actual value"""
    original = {"name": "Alice", "email": None}
    current = {"name": "Alice", "email": "alice@test.com"}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"email": "alice@test.com"}


def test_get_changed_fields_boolean_toggle(mongo_session):
    """Test boolean field toggle"""
    original = {"is_active": True}
    current = {"is_active": False}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"is_active": False}


def test_get_changed_fields_number_zero_vs_nonzero(mongo_session):
    """Test number changing to/from zero"""
    original = {"age": 25}
    current = {"age": 0}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"age": 0}


def test_get_changed_fields_empty_string_vs_populated(mongo_session):
    """Test empty string vs populated"""
    original = {"name": ""}
    current = {"name": "Alice"}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"name": "Alice"}


def test_get_changed_fields_complex_nested_structure(mongo_session):
    """Test complex nested structure change"""
    original = {
        "user": {
            "name": "Alice",
            "roles": ["admin"],
            "settings": {"theme": "dark"},
        },
    }
    current = {
        "user": {
            "name": "Alice",
            "roles": ["admin", "moderator"],
            "settings": {"theme": "light"},
        },
    }

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {
        "user": {
            "name": "Alice",
            "roles": ["admin", "moderator"],
            "settings": {"theme": "light"},
        },
    }


def test_get_changed_fields_list_of_dicts_changed(mongo_session):
    """Test list of dicts change"""
    original = {"items": [{"id": 1, "qty": 2}]}
    current = {"items": [{"id": 1, "qty": 3}]}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"items": [{"id": 1, "qty": 3}]}


def test_get_changed_fields_datetime_changed(mongo_session):
    """Test datetime field change"""
    dt1 = datetime(2025, 1, 1, 12, 0, 0)
    dt2 = datetime(2025, 1, 2, 12, 0, 0)

    original = {"created_at": dt1}
    current = {"created_at": dt2}

    changes = mongo_session._get_changed_fields(original, current)

    assert changes == {"created_at": dt2}


# ============= Tests: add() =============

def test_add_entity_with_id(mongo_session):
    """Test adding entity with _id"""
    user = User(_id="507f1f77bcf86cd799439011", name="Alice", age=25)

    mongo_session.add(user)

    assert User in mongo_session._tracked_entities
    assert "507f1f77bcf86cd799439011" in mongo_session._tracked_entities[User]
    assert User in mongo_session._original_snapshots
    assert "507f1f77bcf86cd799439011" in mongo_session._original_snapshots[User]


def test_add_entity_without_id(mongo_session):
    """Test adding entity without _id (pending insert)"""
    user = User(name="Bob", age=30)

    mongo_session.add(user)

    assert User in mongo_session._pending_inserts
    assert user in mongo_session._pending_inserts[User]
    assert User not in mongo_session._tracked_entities


def test_add_same_entity_twice_no_duplicate(mongo_session):
    """Test adding same entity twice doesn't duplicate"""
    user = User(name="Alice")

    mongo_session.add(user)
    mongo_session.add(user)

    assert len(mongo_session._pending_inserts[User]) == 1


def test_add_non_dataclass_raises_error(mongo_session):
    """Test adding non-dataclass raises error"""
    obj = NotADataclass()

    with pytest.raises(EntityNotDataclassError):
        mongo_session.add(obj)


def test_add_unmapped_entity_raises_error(mongo_session):
    """Test adding entity without collection mapping raises error"""

    @dataclass
    class UnmappedEntity:
        _id: str | None = None
        name: str = ""

    entity = UnmappedEntity(name="test")

    with pytest.raises(CollectionMappingNotFoundError):
        mongo_session.add(entity)


def test_add_snapshot_excludes_id(mongo_session, retort):
    """Test snapshot doesn't include _id"""
    user = User(_id="507f1f77bcf86cd799439011", name="Alice", age=25)

    mongo_session.add(user)

    snapshot = mongo_session._original_snapshots[User]["507f1f77bcf86cd799439011"]
    assert "_id" not in snapshot
    assert snapshot["name"] == "Alice"


def test_add_already_tracked_entity_no_snapshot_overwrite(mongo_session):
    """Test adding already tracked entity doesn't overwrite snapshot"""
    user = User(_id="507f1f77bcf86cd799439011", name="Alice", age=25)

    mongo_session.add(user)
    original_snapshot = mongo_session._original_snapshots[User]["507f1f77bcf86cd799439011"].copy()

    # Modify and add again
    user.age = 30
    mongo_session.add(user)

    # Snapshot should remain unchanged
    assert mongo_session._original_snapshots[User]["507f1f77bcf86cd799439011"] == original_snapshot


def test_add_entity_without_id_field_raises_error(mongo_session):
    """Test EntityMissingIdError for entity without _id field"""

    @dataclass
    class EntityWithoutId:
        name: str = ""

    # Добавляем в mapping
    mongo_session.collection_mapping[EntityWithoutId] = "entities"

    entity = EntityWithoutId(name="test")

    with pytest.raises(EntityMissingIdError) as exc_info:
        mongo_session.add(entity)

    assert "EntityWithoutId" in str(exc_info.value)
    assert "_id" in str(exc_info.value)


def test_add_entity_with_id_field_none_value_works(mongo_session):
    """Test entity with _id field but None value goes to pending"""
    user = User(_id=None, name="Alice")  # _id есть, но None

    mongo_session.add(user)

    assert User in mongo_session._pending_inserts
    assert user in mongo_session._pending_inserts[User]


def test_add_entity_with_id_value_works(mongo_session, valid_object_id):
    """Test entity with _id value goes to tracked"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice")  # _id есть и не None

    mongo_session.add(user)

    assert User in mongo_session._tracked_entities
    assert user_id in mongo_session._tracked_entities[User]


# ============= Tests: add_all() =============

def test_add_all_multiple_entities(mongo_session, valid_object_id):
    """Test adding multiple entities"""
    users = [
        User(_id=valid_object_id(), name="Alice"),
        User(_id=valid_object_id(), name="Bob"),
        User(name="Charlie"),  # No ID
    ]

    mongo_session.add_all(users)

    assert len(mongo_session._tracked_entities[User]) == 2
    assert len(mongo_session._pending_inserts[User]) == 1


def test_add_all_empty_list(mongo_session):
    """Test adding empty list"""
    mongo_session.add_all([])

    assert len(mongo_session._tracked_entities) == 0
    assert len(mongo_session._pending_inserts) == 0


# ============= Tests: flush() - inserts =============

@pytest.mark.asyncio
async def test_flush_pending_inserts(mongo_session, mock_database):
    """Test flush executes pending inserts"""
    # Setup
    user = User(name="Alice", age=25)
    mongo_session.add(user)

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.inserted_id = ObjectId("507f1f77bcf86cd799439011")
    mock_collection.insert_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.flush()

    # Assert
    assert user._id == ObjectId("507f1f77bcf86cd799439011")
    mock_collection.insert_one.assert_called_once()
    assert len(mongo_session._pending_inserts) == 0


@pytest.mark.asyncio
async def test_flush_multiple_pending_inserts(mongo_session, mock_database):
    """Test flush handles multiple inserts"""
    # Setup
    users = [User(name=f"User{i}") for i in range(3)]
    mongo_session.add_all(users)

    mock_collection = AsyncMock()
    mock_results = [
        MagicMock(inserted_id=ObjectId()) for _ in range(3)
    ]
    mock_collection.insert_one.side_effect = mock_results
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.flush()

    # Assert
    assert mock_collection.insert_one.call_count == 3
    for user in users:
        assert user._id is not None


# ============= Tests: flush() - updates =============

@pytest.mark.asyncio
async def test_flush_detects_changes_and_updates(mongo_session, mock_database):
    """Test flush detects changes and performs update"""
    # Setup
    user = User(_id="507f1f77bcf86cd799439011", name="Alice", age=25)
    mongo_session.add(user)

    # Modify
    user.age = 26
    user.email = "alice@test.com"

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.flush()

    # Assert
    mock_collection.update_one.assert_called_once()
    call_args = mock_collection.update_one.call_args

    # Check filter
    assert call_args[0][0] == {"_id": ObjectId("507f1f77bcf86cd799439011")}

    # Check update fields
    update_doc = call_args[0][1]["$set"]
    assert "age" in update_doc
    assert update_doc["age"] == 26
    assert "email" in update_doc
    assert update_doc["email"] == "alice@test.com"
    assert "name" not in update_doc  # Unchanged field


@pytest.mark.asyncio
async def test_flush_no_changes_no_update(mongo_session, mock_database):
    """Test flush doesn't update if no changes"""
    # Setup
    user = User(_id="507f1f77bcf86cd799439011", name="Alice", age=25)
    mongo_session.add(user)

    mock_collection = AsyncMock()
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.flush()

    # Assert
    mock_collection.update_one.assert_not_called()


@pytest.mark.asyncio
async def test_flush_updates_list_field(mongo_session, mock_database):
    """Test flush updates list field correctly"""
    # Setup
    user = User(_id="507f1f77bcf86cd799439011", name="Alice", tags=["python"])
    mongo_session.add(user)

    # Modify list
    user.tags.append("fastapi")

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.flush()

    # Assert
    call_args = mock_collection.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["tags"] == ["python", "fastapi"]


@pytest.mark.asyncio
async def test_flush_updates_dict_field(mongo_session, mock_database):
    """Test flush updates dict field correctly"""
    # Setup
    user = User(_id="507f1f77bcf86cd799439011", name="Alice", metadata={"role": "admin"})
    mongo_session.add(user)

    # Modify dict
    user.metadata["level"] = 5

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.flush()

    # Assert
    call_args = mock_collection.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["metadata"] == {"role": "admin", "level": 5}


@pytest.mark.asyncio
async def test_flush_updates_snapshot_after_success(mongo_session, mock_database):
    """Test flush updates snapshot after successful update"""
    # Setup
    user = User(_id="507f1f77bcf86cd799439011", name="Alice", age=25)
    mongo_session.add(user)
    original_snapshot = mongo_session._original_snapshots[User]["507f1f77bcf86cd799439011"].copy()

    # Modify
    user.age = 26

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.flush()

    # Assert - snapshot should be updated
    new_snapshot = mongo_session._original_snapshots[User]["507f1f77bcf86cd799439011"]
    assert new_snapshot["age"] == 26
    assert new_snapshot != original_snapshot


@pytest.mark.asyncio
async def test_flush_invalid_object_id_raises_error(mongo_session, mock_database):
    """Test flush with invalid ObjectId raises error"""
    # Setup - manually create bad state
    user = User(name="Alice")
    user._id = "invalid_object_id"

    mongo_session._tracked_entities[User] = {"invalid_object_id": user}
    mongo_session._original_snapshots[User] = {
        "invalid_object_id": {"name": "Alice", "age": 0, "email": "", "tags": [], "metadata": {}},
    }

    user.age = 25  # Make a change

    mock_database.__getitem__.return_value = AsyncMock()

    # Act & Assert
    with pytest.raises(InvalidEntityIdError):
        await mongo_session.flush()


# ============= Tests: commit() =============

@pytest.mark.asyncio
async def test_commit_flushes_and_commits_transaction(mongo_session, mock_session, mock_database):
    """Test commit flushes changes and commits transaction"""
    # Setup
    user = User(name="Alice")
    mongo_session.add(user)

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.inserted_id = ObjectId()
    mock_collection.insert_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.commit()

    # Assert
    mock_session.commit_transaction.assert_called_once()
    assert len(mongo_session._tracked_entities) == 0
    assert len(mongo_session._original_snapshots) == 0


@pytest.mark.asyncio
async def test_commit_clears_tracking(mongo_session, mock_session):
    """Test commit clears all tracking structures"""
    # Setup
    user = User(_id="507f1f77bcf86cd799439011", name="Alice")
    mongo_session.add(user)

    # Act
    await mongo_session.commit()

    # Assert
    assert len(mongo_session._tracked_entities) == 0
    assert len(mongo_session._original_snapshots) == 0
    assert len(mongo_session._pending_inserts) == 0


# ============= Tests: rollback() =============

@pytest.mark.asyncio
async def test_rollback_aborts_transaction(mongo_session, mock_session):
    """Test rollback aborts transaction"""
    # Setup
    user = User(name="Alice")
    mongo_session.add(user)

    # Act
    await mongo_session.rollback()

    # Assert
    mock_session.abort_transaction.assert_called_once()
    assert len(mongo_session._tracked_entities) == 0
    assert len(mongo_session._pending_inserts) == 0


@pytest.mark.asyncio
async def test_rollback_clears_all_tracking(mongo_session, mock_session, valid_object_id):
    """Test rollback clears all tracking structures"""
    # Setup
    user1 = User(_id=valid_object_id(), name="Alice")
    user2 = User(name="Bob")
    mongo_session.add(user1)
    mongo_session.add(user2)

    # Act
    await mongo_session.rollback()

    # Assert
    assert len(mongo_session._tracked_entities) == 0
    assert len(mongo_session._original_snapshots) == 0
    assert len(mongo_session._pending_inserts) == 0


# ============= Edge Cases =============

@pytest.mark.asyncio
async def test_flush_empty_session(mongo_session):
    """Test flush with no tracked entities"""
    await mongo_session.flush()
    # Should not raise any errors


def test_multiple_entity_types_tracked(mongo_session, valid_object_id):
    """Test tracking multiple entity types simultaneously"""
    user = User(_id=valid_object_id(), name="Alice")
    product = Product(_id=valid_object_id(), title="Book")

    mongo_session.add(user)
    mongo_session.add(product)

    assert len(mongo_session._tracked_entities) == 2
    assert User in mongo_session._tracked_entities
    assert Product in mongo_session._tracked_entities


@pytest.mark.asyncio
async def test_flush_handles_multiple_entity_types(mongo_session, mock_database, valid_object_id):
    """Test flush handles updates for multiple entity types"""
    # Setup - используем валидные ObjectId
    user_id = valid_object_id()
    product_id = valid_object_id()

    user = User(_id=user_id, name="Alice", age=25)
    product = Product(_id=product_id, title="Book", price=10.0)

    mongo_session.add(user)
    mongo_session.add(product)

    user.age = 26
    product.price = 15.0

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    await mongo_session.flush()

    # Assert
    assert mock_collection.update_one.call_count == 2


def test_entity_id_converted_to_string(mongo_session):
    """Test entity _id is converted to string for tracking"""
    object_id = ObjectId("507f1f77bcf86cd799439011")
    user = User(_id=object_id, name="Alice")

    mongo_session.add(user)

    # ID должен быть преобразован в строку
    assert "507f1f77bcf86cd799439011" in mongo_session._tracked_entities[User]


# ============= Integration Tests: Real-world Scenarios =============

@pytest.mark.asyncio
async def test_scenario_create_user_and_insert(mongo_session, mock_database):
    """Scenario: Create new user and insert into DB"""
    # Arrange - создаем нового пользователя без ID
    user = User(
        name="Alice Smith",
        email="alice@example.com",
        age=28,
        tags=["python", "fastapi"],
        metadata={"role": "developer", "level": "senior"},
    )

    mock_collection = AsyncMock()
    generated_id = ObjectId()
    mock_result = MagicMock()
    mock_result.inserted_id = generated_id
    mock_collection.insert_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    mongo_session.add(user)
    await mongo_session.flush()

    # Assert - проверяем вызов insert_one
    mock_collection.insert_one.assert_called_once()
    insert_doc = mock_collection.insert_one.call_args[0][0]

    assert "_id" not in insert_doc  # MongoDB сам генерирует _id
    assert insert_doc["name"] == "Alice Smith"
    assert insert_doc["email"] == "alice@example.com"
    assert insert_doc["age"] == 28
    assert insert_doc["tags"] == ["python", "fastapi"]
    assert insert_doc["metadata"] == {"role": "developer", "level": "senior"}
    assert user._id == generated_id


@pytest.mark.asyncio
async def test_scenario_update_simple_fields(mongo_session, mock_database, valid_object_id):
    """Scenario: Update simple scalar fields"""
    # Arrange - существующий пользователь
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        email="old@example.com",
        age=25,
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - изменяем простые поля
    mongo_session.add(user)
    user.name = "Alice Smith"
    user.email = "new@example.com"
    user.age = 26
    await mongo_session.flush()

    # Assert - проверяем update_one
    mock_collection.update_one.assert_called_once()
    filter_doc, update_doc = mock_collection.update_one.call_args[0][:2]

    assert filter_doc == {"_id": ObjectId(user_id)}
    assert "$set" in update_doc

    set_fields = update_doc["$set"]
    assert set_fields["name"] == "Alice Smith"
    assert set_fields["email"] == "new@example.com"
    assert set_fields["age"] == 26
    assert "tags" not in set_fields  # Неизмененные поля не в $set
    assert "metadata" not in set_fields


@pytest.mark.asyncio
async def test_scenario_add_items_to_list(mongo_session, mock_database, valid_object_id):
    """Scenario: Add items to list field"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        tags=["python"],
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - добавляем элементы в список
    mongo_session.add(user)
    user.tags.append("fastapi")
    user.tags.append("mongodb")
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["tags"] == ["python", "fastapi", "mongodb"]


@pytest.mark.asyncio
async def test_scenario_remove_items_from_list(mongo_session, mock_database, valid_object_id):
    """Scenario: Remove items from list"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        tags=["python", "fastapi", "django", "flask"],
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - удаляем элементы
    mongo_session.add(user)
    user.tags.remove("django")
    user.tags.remove("flask")
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["tags"] == ["python", "fastapi"]


@pytest.mark.asyncio
async def test_scenario_clear_list(mongo_session, mock_database, valid_object_id):
    """Scenario: Clear entire list"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        tags=["python", "fastapi", "mongodb"],
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - очищаем список
    mongo_session.add(user)
    user.tags.clear()
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["tags"] == []


@pytest.mark.asyncio
async def test_scenario_reorder_list(mongo_session, mock_database, valid_object_id):
    """Scenario: Reorder list items"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        tags=["python", "fastapi", "mongodb"],
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - меняем порядок
    mongo_session.add(user)
    user.tags.sort()  # Сортируем: ["fastapi", "mongodb", "python"]
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["tags"] == ["fastapi", "mongodb", "python"]


@pytest.mark.asyncio
async def test_scenario_update_dict_add_key(mongo_session, mock_database, valid_object_id):
    """Scenario: Add key to dict field"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={"role": "developer"},
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - добавляем ключ в dict
    mongo_session.add(user)
    user.metadata["level"] = "senior"
    user.metadata["years_experience"] = 5
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["metadata"] == {
        "role": "developer",
        "level": "senior",
        "years_experience": 5,
    }


@pytest.mark.asyncio
async def test_scenario_update_dict_modify_value(mongo_session, mock_database, valid_object_id):
    """Scenario: Modify dict value"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={"role": "junior", "level": 1},
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - изменяем значения в dict
    mongo_session.add(user)
    user.metadata["role"] = "senior"
    user.metadata["level"] = 5
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["metadata"] == {"role": "senior", "level": 5}


@pytest.mark.asyncio
async def test_scenario_update_dict_remove_key(mongo_session, mock_database, valid_object_id):
    """Scenario: Remove key from dict"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={"role": "developer", "temp_flag": True, "level": 3},
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - удаляем ключ
    mongo_session.add(user)
    del user.metadata["temp_flag"]
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["metadata"] == {"role": "developer", "level": 3}
    assert "temp_flag" not in set_fields["metadata"]


@pytest.mark.asyncio
async def test_scenario_nested_dict_modification(mongo_session, mock_database, valid_object_id):
    """Scenario: Modify nested dict structure"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={
            "profile": {
                "avatar": "old_avatar.jpg",
                "bio": "Developer",
            },
            "settings": {
                "theme": "dark",
                "notifications": True,
            },
        },
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - изменяем вложенный dict
    mongo_session.add(user)
    user.metadata["profile"]["avatar"] = "new_avatar.jpg"
    user.metadata["settings"]["theme"] = "light"
    await mongo_session.flush()

    # Assert - вся структура metadata должна быть в $set
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["metadata"]["profile"]["avatar"] == "new_avatar.jpg"
    assert set_fields["metadata"]["settings"]["theme"] == "light"


@pytest.mark.asyncio
async def test_scenario_list_of_dicts(mongo_session, mock_database, valid_object_id):
    """Scenario: Work with list of dicts (like Order items)"""
    # Arrange
    order_id = valid_object_id()
    order = Order(
        _id=order_id,
        user_id="user123",
        items=[
            {"product_id": "p1", "quantity": 2, "price": 10.0},
            {"product_id": "p2", "quantity": 1, "price": 20.0},
        ],
        total=40.0,
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - изменяем список словарей
    mongo_session.add(order)
    order.items[0]["quantity"] = 3  # Увеличиваем количество
    order.items.append({"product_id": "p3", "quantity": 1, "price": 15.0})  # Добавляем товар
    order.total = 65.0
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert len(set_fields["items"]) == 3
    assert set_fields["items"][0]["quantity"] == 3
    assert set_fields["items"][2]["product_id"] == "p3"
    assert set_fields["total"] == 65.0


@pytest.mark.asyncio
async def test_scenario_complex_nested_update(mongo_session, mock_database, valid_object_id):
    """Scenario: Complex nested structure update"""
    # Arrange - сложная вложенная структура
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={
            "projects": [
                {
                    "name": "Project A",
                    "tags": ["python", "api"],
                    "status": "active",
                },
                {
                    "name": "Project B",
                    "tags": ["javascript"],
                    "status": "completed",
                },
            ],
            "skills": {
                "languages": ["Python", "JavaScript"],
                "frameworks": ["FastAPI", "React"],
            },
        },
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - множественные изменения
    mongo_session.add(user)

    # Изменяем проект
    user.metadata["projects"][0]["status"] = "on_hold"
    user.metadata["projects"][0]["tags"].append("mongodb")

    # Добавляем новый проект
    user.metadata["projects"].append({
        "name": "Project C",
        "tags": ["go"],
        "status": "active",
    })

    # Обновляем навыки
    user.metadata["skills"]["languages"].append("Go")

    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]

    # Проверяем проекты
    projects = set_fields["metadata"]["projects"]
    assert len(projects) == 3
    assert projects[0]["status"] == "on_hold"
    assert "mongodb" in projects[0]["tags"]
    assert projects[2]["name"] == "Project C"

    # Проверяем навыки
    assert "Go" in set_fields["metadata"]["skills"]["languages"]


@pytest.mark.asyncio
async def test_scenario_replace_entire_list(mongo_session, mock_database, valid_object_id):
    """Scenario: Replace entire list with new one"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        tags=["old1", "old2", "old3"],
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - полная замена списка
    mongo_session.add(user)
    user.tags = ["new1", "new2"]
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["tags"] == ["new1", "new2"]


@pytest.mark.asyncio
async def test_scenario_replace_entire_dict(mongo_session, mock_database, valid_object_id):
    """Scenario: Replace entire dict with new one"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={"old_key": "old_value", "to_remove": True},
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - полная замена dict
    mongo_session.add(user)
    user.metadata = {"new_key": "new_value", "another": 123}
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["metadata"] == {"new_key": "new_value", "another": 123}


@pytest.mark.asyncio
async def test_scenario_mixed_changes(mongo_session, mock_database, valid_object_id):
    """Scenario: Mix of scalar, list, and dict changes"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        email="old@example.com",
        age=25,
        tags=["python"],
        metadata={"role": "junior"},
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - множественные изменения разных типов
    mongo_session.add(user)
    user.name = "Alice Smith"  # scalar
    user.age = 26  # scalar
    user.tags.append("fastapi")  # list
    user.metadata["role"] = "senior"  # dict
    user.metadata["certified"] = True  # dict - new key
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["name"] == "Alice Smith"
    assert set_fields["age"] == 26
    assert set_fields["tags"] == ["python", "fastapi"]
    assert set_fields["metadata"]["role"] == "senior"
    assert set_fields["metadata"]["certified"] is True
    assert "email" not in set_fields  # Unchanged field


@pytest.mark.asyncio
async def test_scenario_no_changes(mongo_session, mock_database, valid_object_id):
    """Scenario: Add entity but make no changes"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        age=25,
    )

    mock_collection = AsyncMock()
    mock_database.__getitem__.return_value = mock_collection

    # Act - НЕ изменяем ничего
    mongo_session.add(user)
    await mongo_session.flush()

    # Assert - update_one НЕ должен вызываться
    mock_collection.update_one.assert_not_called()


@pytest.mark.asyncio
async def test_scenario_datetime_field_update(mongo_session, mock_database, valid_object_id):
    """Scenario: Update datetime field"""
    # Arrange
    user_id = valid_object_id()
    original_time = datetime(2025, 1, 1, 12, 0, 0)
    user = User(
        _id=user_id,
        name="Alice",
        created_at=original_time,
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - изменяем datetime
    mongo_session.add(user)
    new_time = datetime(2025, 12, 31, 23, 59, 59)
    user.created_at = new_time
    await mongo_session.flush()

    # Assert - Retort сериализует datetime в ISO string по умолчанию
    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]

    assert "created_at" in set_fields
    # Retort конвертирует datetime в ISO string
    assert set_fields["created_at"] == "2025-12-31T23:59:59"
    # Или проверяем что это валидная ISO строка
    assert isinstance(set_fields["created_at"], str)


@pytest.mark.asyncio
async def test_scenario_empty_list_to_populated_list(mongo_session, mock_database, valid_object_id):
    """Scenario: Change empty list to populated"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        tags=[],
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    mongo_session.add(user)
    user.tags = ["python", "fastapi", "mongodb"]
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["tags"] == ["python", "fastapi", "mongodb"]


@pytest.mark.asyncio
async def test_scenario_empty_dict_to_populated_dict(mongo_session, mock_database, valid_object_id):
    """Scenario: Change empty dict to populated"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={},
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    mongo_session.add(user)
    user.metadata = {"role": "developer", "level": 5}
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["metadata"] == {"role": "developer", "level": 5}


@pytest.mark.asyncio
async def test_scenario_list_with_duplicates(mongo_session, mock_database, valid_object_id):
    """Scenario: List containing duplicate values"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        tags=["python"],
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - добавляем дубликаты
    mongo_session.add(user)
    user.tags.append("python")  # Дубликат
    user.tags.append("fastapi")
    user.tags.append("python")  # Еще дубликат
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["tags"] == ["python", "python", "fastapi", "python"]


@pytest.mark.asyncio
async def test_scenario_deeply_nested_structure(mongo_session, mock_database, valid_object_id):
    """Scenario: Very deeply nested structure (3+ levels)"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={
            "company": {
                "name": "TechCorp",
                "departments": {
                    "engineering": {
                        "teams": ["backend", "frontend"],
                        "lead": "Bob",
                    },
                },
            },
        },
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - изменяем глубоко вложенное значение
    mongo_session.add(user)
    user.metadata["company"]["departments"]["engineering"]["teams"].append("devops")
    user.metadata["company"]["departments"]["engineering"]["lead"] = "Charlie"
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    engineering = set_fields["metadata"]["company"]["departments"]["engineering"]
    assert engineering["teams"] == ["backend", "frontend", "devops"]
    assert engineering["lead"] == "Charlie"


@pytest.mark.asyncio
async def test_scenario_multiple_flushes(mongo_session, mock_database, valid_object_id):
    """Scenario: Multiple flushes with different changes"""
    # Arrange
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        age=25,
        tags=[],
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act & Assert - First flush
    mongo_session.add(user)
    user.age = 26
    await mongo_session.flush()

    first_call_set_fields = mock_collection.update_one.call_args_list[0][0][1]["$set"]
    assert first_call_set_fields == {"age": 26}

    # Act & Assert - Second flush with new changes
    user.tags.append("python")
    await mongo_session.flush()

    second_call_set_fields = mock_collection.update_one.call_args_list[1][0][1]["$set"]
    assert second_call_set_fields == {"tags": ["python"]}

    # Total calls
    assert mock_collection.update_one.call_count == 2


@pytest.mark.asyncio
async def test_scenario_boolean_field_toggle(mongo_session, mock_database, valid_object_id):
    """Scenario: Toggle boolean field"""
    # Arrange
    product_id = valid_object_id()
    product = Product(
        _id=product_id,
        title="Book",
        price=10.0,
        in_stock=True,
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - переключаем boolean
    mongo_session.add(product)
    product.in_stock = False
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["in_stock"] is False


@pytest.mark.asyncio
async def test_scenario_numeric_zero_values(mongo_session, mock_database, valid_object_id):
    """Scenario: Set numeric fields to zero"""
    # Arrange
    product_id = valid_object_id()
    product = Product(
        _id=product_id,
        title="Book",
        price=10.0,
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Act - устанавливаем в 0
    mongo_session.add(product)
    product.price = 0.0
    await mongo_session.flush()

    # Assert
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["price"] == 0.0


@pytest.mark.asyncio
async def test_scenario_batch_insert_and_update(mongo_session, mock_database, valid_object_id):
    """Scenario: Mix of inserts and updates in one flush"""
    # Arrange
    existing_user_id = valid_object_id()
    existing_user = User(_id=existing_user_id, name="Alice", age=25)
    new_user = User(name="Bob", age=30)

    mock_collection = AsyncMock()

    # Mock insert
    insert_result = MagicMock()
    insert_result.inserted_id = ObjectId()

    # Mock update
    update_result = MagicMock()
    update_result.modified_count = 1

    mock_collection.insert_one.return_value = insert_result
    mock_collection.update_one.return_value = update_result
    mock_database.__getitem__.return_value = mock_collection

    # Act
    mongo_session.add(existing_user)
    mongo_session.add(new_user)

    existing_user.age = 26  # Update
    await mongo_session.flush()

    # Assert
    mock_collection.insert_one.assert_called_once()  # Bob inserted
    mock_collection.update_one.assert_called_once()  # Alice updated

    # Check insert
    insert_doc = mock_collection.insert_one.call_args[0][0]
    assert insert_doc["name"] == "Bob"
    assert insert_doc["age"] == 30

    # Check update
    update_set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert update_set_fields["age"] == 26

# ====================== ДОПОЛНИТЕЛЬНЫЕ ИНТЕГРАЦИОННЫЕ ТЕСТЫ ======================

# 1. Optional / None поля

@pytest.mark.asyncio
async def test_scenario_optional_field_set_to_none(mongo_session, mock_database, valid_object_id):
    """Scenario: Set optional field to None"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", email="alice@test.com")

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.email = None  # Устанавливаем в None
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["email"] is None


@pytest.mark.asyncio
async def test_scenario_optional_field_from_none_to_value(mongo_session, mock_database, valid_object_id):
    """Scenario: Change None to actual value"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", email=None)

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.email = "alice@test.com"
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["email"] == "alice@test.com"


# 2. Вложенные списки

@pytest.mark.asyncio
async def test_scenario_nested_list_modification(mongo_session, mock_database, valid_object_id):
    """Scenario: List containing lists"""
    user_id = valid_object_id()
    user = User(
        _id=user_id,
        name="Alice",
        metadata={"matrix": [[1, 2], [3, 4]]},
    )

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.metadata = {"matrix": [[1, 2], [3, 4], [5, 6]]}
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["metadata"]["matrix"] == [[1, 2], [3, 4], [5, 6]]


# 3. Пустые строки и пробелы

@pytest.mark.asyncio
async def test_scenario_empty_string_to_whitespace(mongo_session, mock_database, valid_object_id):
    """Scenario: Empty string vs whitespace string"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="")

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.name = "   "  # Только пробелы
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["name"] == "   "


@pytest.mark.asyncio
async def test_scenario_trim_string(mongo_session, mock_database, valid_object_id):
    """Scenario: String with leading/trailing spaces"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="  Alice  ")

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.name = "Alice"  # Обрезали пробелы
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["name"] == "Alice"


# 4. Числа с плавающей точкой и отрицательные значения

@pytest.mark.asyncio
async def test_scenario_float_precision(mongo_session, mock_database, valid_object_id):
    """Scenario: Float precision changes"""
    product_id = valid_object_id()
    product = Product(_id=product_id, title="Book", price=10.99)

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(product)
    product.price = 10.999999999
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert "price" in set_fields


@pytest.mark.asyncio
async def test_scenario_negative_numbers(mongo_session, mock_database, valid_object_id):
    """Scenario: Negative numbers"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", age=25)

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.age = -1
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["age"] == -1


# 5. Большие коллекции

@pytest.mark.asyncio
async def test_scenario_large_list(mongo_session, mock_database, valid_object_id):
    """Scenario: Large list (1000 items)"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", tags=[])

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.tags = [f"tag_{i}" for i in range(1000)]
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert len(set_fields["tags"]) == 1000


@pytest.mark.asyncio
async def test_scenario_large_dict(mongo_session, mock_database, valid_object_id):
    """Scenario: Large dict (1000 keys)"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", metadata={})

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.metadata = {f"key_{i}": f"value_{i}" for i in range(1000)}
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert len(set_fields["metadata"]) == 1000


# 6. Unicode и спецсимволы

@pytest.mark.asyncio
async def test_scenario_unicode_strings(mongo_session, mock_database, valid_object_id):
    """Scenario: Unicode characters in strings"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice")

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.name = "Алиса 中文 🎉 émojis"
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["name"] == "Алиса 中文 🎉 émojis"


@pytest.mark.asyncio
async def test_scenario_special_characters_in_dict_keys(mongo_session, mock_database, valid_object_id):
    """Scenario: Special characters in dict keys"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", metadata={})

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.metadata = {
        "key-with-dash": "value1",
        "key.with.dot": "value2",  # потенциальная проблема в реальной Mongo
        "key_with_underscore": "value3",
    }
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert "key-with-dash" in set_fields["metadata"]
    assert "key_with_underscore" in set_fields["metadata"]
    # ключ с точкой в реальной Mongo приведет к ошибке, но здесь мы лишь проверяем формирование update-документа


# 7. Изменение типа/значения поля (последнее значение)

@pytest.mark.asyncio
async def test_scenario_multiple_modifications_same_field(mongo_session, mock_database, valid_object_id):
    """Scenario: Modify same field multiple times before flush"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", age=25)

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    mongo_session.add(user)
    user.age = 26
    user.age = 27
    user.age = 28  # последнее значение
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["age"] == 28


# 8. Цикл: изменения, rollback, новые изменения

@pytest.mark.asyncio
async def test_scenario_changes_after_rollback(mongo_session, mock_database, mock_session, valid_object_id):
    """Scenario: Make changes, rollback, make new changes"""
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", age=25)

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Первый заход
    mongo_session.add(user)
    user.age = 26
    await mongo_session.rollback()
    mock_session.abort_transaction.assert_called_once()

    # Второй заход с новым трекингом
    mongo_session.add(user)
    user.age = 30
    await mongo_session.flush()

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert set_fields["age"] == 30


# ====================== ТЕСТЫ КОТОРЫЕ СЛОМАЮТ ПОВЕДЕНИЕ ======================

@pytest.mark.asyncio
async def test_breaking_concurrent_modification_same_entity(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: Concurrent modification of same entity reference

    Проблема: Если один и тот же entity объект добавлен дважды с разными ID,
    система может неправильно отследить изменения.

    Ожидаемое поведение: Должно работать корректно или бросать ошибку.
    Реальное поведение: Может привести к потере данных или неконсистентности.
    """
    user_id_1 = valid_object_id()
    user_id_2 = valid_object_id()

    # Создаем одного пользователя
    user = User(_id=user_id_1, name="Alice", age=25)
    mongo_session.add(user)

    # Меняем его ID и добавляем снова
    user._id = user_id_2  # ВНИМАНИЕ: меняем ID у tracked entity!
    mongo_session.add(user)

    # Теперь изменяем данные
    user.age = 30
    user.name = "Bob"

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    await mongo_session.flush()

    # ПРОБЛЕМА: Какой ID будет в update_one?
    # user_id_1 или user_id_2?
    # Оба записи должны обновиться или только одна?

    # Этот тест покажет, что происходит при изменении _id у tracked entity
    # Ожидаемое: должна быть ошибка или четкое поведение
    # Реальное: возможна неконсистентность

    # Проверяем количество вызовов update_one
    # ДОЛЖНО быть 2 вызова (для каждого ID), но скорее всего будет 1
    assert mock_collection.update_one.call_count == 2, \
        f"Expected 2 updates, got {mock_collection.update_one.call_count}"


@pytest.mark.asyncio
async def test_breaking_snapshot_reference_pollution(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: Snapshot pollution через shared mutable objects

    Проблема: Если в entity есть mutable объекты (list, dict) и они shared,
    deepcopy должен защитить, но может быть edge case.

    Реальный сценарий: Два entity используют один и тот же list/dict объект.
    """
    user_id_1 = valid_object_id()
    user_id_2 = valid_object_id()

    # Создаем SHARED metadata объект
    shared_metadata = {"role": "admin", "level": 5}
    shared_tags = ["python", "fastapi"]

    user1 = User(_id=user_id_1, name="Alice", metadata=shared_metadata, tags=shared_tags)
    user2 = User(_id=user_id_2, name="Bob", metadata=shared_metadata, tags=shared_tags)

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Добавляем оба
    mongo_session.add(user1)
    mongo_session.add(user2)

    # Изменяем через user1 - должно повлиять на user2!
    user1.metadata["role"] = "superadmin"
    user1.tags.append("mongodb")

    await mongo_session.flush()

    # ПРОБЛЕМА: Оба пользователя используют одни и те же объекты в памяти
    # Изменения в user1 автоматически меняют user2!

    # После flush оба должны иметь обновленные данные
    assert user1.metadata["role"] == "superadmin"
    assert user2.metadata["role"] == "superadmin"  # НЕОЖИДАННО!

    assert "mongodb" in user1.tags
    assert "mongodb" in user2.tags  # НЕОЖИДАННО!

    # Проверяем что оба были обновлены в БД
    assert mock_collection.update_one.call_count == 2

    # Проверяем что оба update содержат одинаковые изменения
    calls = mock_collection.update_one.call_args_list
    set_fields_1 = calls[0][0][1]["$set"]
    set_fields_2 = calls[1][0][1]["$set"]

    # BREAKING: Оба update будут содержать одинаковые изменения!
    assert set_fields_1["metadata"]["role"] == "superadmin"
    assert set_fields_2["metadata"]["role"] == "superadmin"

    # Это может быть неожиданным поведением для пользователя
    # Должна быть защита или явное предупреждение


@pytest.mark.asyncio
async def test_breaking_flush_after_exception_state_corruption(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: State corruption after partial flush failure

    Проблема: Если flush частично выполнился (некоторые updates прошли),
    а затем упал с ошибкой, snapshot может обновиться неконсистентно.
    """
    user_id_1 = valid_object_id()
    user_id_2 = valid_object_id()

    user1 = User(_id=user_id_1, name="Alice", age=25)
    user2 = User(_id=user_id_2, name="Bob", age=30)

    mongo_session.add(user1)
    mongo_session.add(user2)

    user1.age = 26
    user2.age = 31

    mock_collection = AsyncMock()

    # Первый update проходит успешно
    success_result = MagicMock()
    success_result.modified_count = 1

    # Второй update падает с ошибкой
    mock_collection.update_one.side_effect = [
        success_result,  # user1 обновился
        Exception("Database connection lost"),  # user2 упал
    ]
    mock_database.__getitem__.return_value = mock_collection

    # Пытаемся flush
    with pytest.raises(Exception, match="Database connection lost"):
        await mongo_session.flush()

    # ПРОБЛЕМА: Что со snapshot после ошибки?
    # user1 обновился в БД, но snapshot обновился?
    # user2 не обновился в БД, но snapshot мог измениться?

    # Проверяем состояние snapshots
    snapshot1 = mongo_session._original_snapshots[User][user_id_1]
    snapshot2 = mongo_session._original_snapshots[User][user_id_2]

    # BREAKING: Snapshot может быть в неконсистентном состоянии
    # Либо оба обновились (неправильно, т.к. user2 упал)
    # Либо оба не обновились (неправильно, т.к. user1 прошел)
    # Либо только user1 обновился (правильно, но сложно реализовать)

    # Попытка повторного flush должна быть идемпотентной
    mock_collection.update_one.side_effect = None
    mock_collection.update_one.return_value = success_result

    await mongo_session.flush()

    # Должно обновиться только то, что не обновилось ранее
    # Но из-за некорректного snapshot может обновиться всё снова


# ====================== БОЛЕЕ АГРЕССИВНЫЕ BREAKING ТЕСТЫ ======================

@pytest.mark.asyncio
async def test_breaking_modify_entity_between_add_and_flush(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: Изменение entity между add() и flush() должно игнорироваться

    Проблема: Если изменить entity ПЕРЕД add(), то snapshot захватит уже измененное состояние.
    Это означает, что flush() не увидит изменений.
    """
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", age=25)

    # ВАЖНО: изменяем ДО add()
    user.age = 26
    user.name = "Bob"

    # Теперь добавляем - snapshot зафиксирует age=26, name="Bob"
    mongo_session.add(user)

    # Изменяем еще раз ПОСЛЕ add()
    user.age = 30
    user.name = "Charlie"

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    await mongo_session.flush()

    # Ожидание: update должен содержать изменения с 26->30 и Bob->Charlie
    # Реальность: если первые изменения (до add) не учтены в snapshot,
    # то БД получит неверные данные

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]

    # BREAKING: Эти изменения должны быть относительно ОРИГИНАЛЬНОГО состояния (Alice, 25)
    # А не того, что было на момент add() (Bob, 26)
    assert set_fields["age"] == 30
    assert set_fields["name"] == "Charlie"

    # Но если в БД было (Alice, 25), то update приведет к (Charlie, 30)
    # А если в БД было (Bob, 26), то update тоже приведет к (Charlie, 30)
    # Но БД состояние может быть любым!


@pytest.mark.asyncio
async def test_breaking_missing_deepcopy_exposes_mutation(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: Проверка что deepcopy действительно работает

    Если убрать deepcopy из add(), этот тест должен упасть.
    """
    user_id = valid_object_id()
    nested_obj = {"level": {"value": 5}}
    user = User(_id=user_id, name="Alice", metadata=nested_obj)

    mongo_session.add(user)

    # Изменяем ОРИГИНАЛЬНЫЙ объект nested_obj
    nested_obj["level"]["value"] = 10

    # Также изменяем через user.metadata (это тот же объект!)
    user.metadata["role"] = "admin"

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    await mongo_session.flush()

    # Без deepcopy: snapshot тоже изменится, flush не увидит изменений
    # С deepcopy: snapshot защищен, изменения детектируются

    mock_collection.update_one.assert_called_once()
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]

    # Должны увидеть изменения
    assert set_fields["metadata"]["level"]["value"] == 10
    assert set_fields["metadata"]["role"] == "admin"


@pytest.mark.asyncio
async def test_breaking_update_one_returns_zero_modified(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: update_one возвращает modified_count = 0 (документ не найден)

    Проблема: Если документ был удален между add() и flush(),
    update_one вернет modified_count=0, но система не среагирует.
    """
    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice", age=25)

    mongo_session.add(user)
    user.age = 26

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 0  # Документ НЕ найден/обновлен!
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # Flush НЕ должен бросить ошибку при modified_count=0
    # Но возможно ДОЛЖЕН бросить, т.к. это аномалия
    await mongo_session.flush()

    # BREAKING: Система не знает, что update не прошел
    # Snapshot обновится, повторный flush не увидит изменений

    # Snapshot должен быть обновлен
    snapshot = mongo_session._original_snapshots[User][user_id]
    assert snapshot["age"] == 26  # Обновился

    # Но в реальной БД age=25 (update не прошел)!
    # Это data loss!


@pytest.mark.asyncio
async def test_breaking_entity_without_id_field(mongo_session, mock_database):
    """FIXED: Test entity without _id field raises EntityMissingIdError"""

    @dataclass
    class EntityWithoutId:
        name: str = ""
        value: int = 0

    # Добавляем в mapping
    mongo_session.collection_mapping[EntityWithoutId] = "entities"

    entity = EntityWithoutId(name="test", value=123)

    # Теперь должна быть правильная ошибка
    with pytest.raises(EntityMissingIdError):
        mongo_session.add(entity)


@pytest.mark.asyncio
async def test_breaking_snapshot_deepcopy_with_unpicklable_object(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: Entity содержит объект, который нельзя скопировать

    Некоторые объекты (file handles, locks, connections) нельзя deepcopy.
    """
    import threading

    user_id = valid_object_id()
    user = User(_id=user_id, name="Alice")

    # Добавляем unpicklable объект
    user.metadata = {
        "lock": threading.Lock(),  # Нельзя deepcopy!
        "value": 123,
    }

    # BREAKING: deepcopy упадет с ошибкой
    with pytest.raises(TypeError, match="cannot pickle"):
        mongo_session.add(user)


@pytest.mark.asyncio
async def test_breaking_retort_serialization_changes_structure(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: Retort изменяет структуру при сериализации

    Если Retort добавляет/удаляет поля, сравнение snapshot может быть некорректным.
    """
    user_id = valid_object_id()

    # Создаем entity с datetime
    user = User(_id=user_id, name="Alice", created_at=datetime(2025, 1, 1, 12, 0, 0))

    mongo_session.add(user)

    # Проверяем snapshot - datetime должен быть сериализован
    snapshot = mongo_session._original_snapshots[User][user_id]

    # BREAKING: Если snapshot содержит datetime как строку,
    # а current_dump содержит datetime как объект, сравнение упадет

    # В текущей реализации retort.dump() вызывается дважды:
    # 1. При создании snapshot
    # 2. При flush для current_dump
    # Они должны давать идентичный результат

    assert "created_at" in snapshot
    # Retort сериализует datetime в ISO string
    assert isinstance(snapshot["created_at"], str)

    # Изменяем что-то еще
    user.name = "Bob"

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    await mongo_session.flush()

    # Только name должен быть в $set, created_at не должен
    set_fields = mock_collection.update_one.call_args[0][1]["$set"]
    assert "name" in set_fields
    assert "created_at" not in set_fields  # НЕ изменялся


@pytest.mark.asyncio
async def test_breaking_massive_concurrent_updates(mongo_session, mock_database, valid_object_id):
    """
    BREAKING TEST: Стресс-тест с большим количеством entities

    100+ entities с изменениями могут выявить проблемы производительности или памяти.
    """
    # Создаем 100 пользователей
    users = []
    for i in range(100):
        user_id = valid_object_id()
        user = User(_id=user_id, name=f"User{i}", age=20 + i)
        users.append(user)
        mongo_session.add(user)

    # Изменяем каждого
    for i, user in enumerate(users):
        user.age += 10
        user.tags.append(f"tag_{i}")
        user.metadata = {"index": i, "data": [1, 2, 3] * 100}  # Большой объект

    mock_collection = AsyncMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    mock_collection.update_one.return_value = mock_result
    mock_database.__getitem__.return_value = mock_collection

    # BREAKING: Может упасть от нехватки памяти или таймаута
    await mongo_session.flush()

    # Должно быть 100 вызовов update_one
    assert mock_collection.update_one.call_count == 100

    # Проверяем что все snapshots обновились
    for i, user in enumerate(users):
        snapshot = mongo_session._original_snapshots[User][user._id]
        assert snapshot["age"] == 30 + i



if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
