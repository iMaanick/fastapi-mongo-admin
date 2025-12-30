import re
from collections.abc import Callable
from typing import Any

from bson import ObjectId

from app.application.exceptions.base import InvalidQueryOperatorError


def build_mongo_filter(where: dict[str, Any] | str | None) -> dict[str, Any]:
    """
    Преобразует where из starlette-admin в MongoDB filter

    Примеры where:
    - {"username": {"contains": "john"}}
    - {"is_active": {"eq": True}}
    - {"or": [{"username": {"eq": "john"}}, {"email": {"contains": "test"}}]}
    """
    if where is None:
        return {}

    if isinstance(where, str):
        # Full-text search по всем полям
        regex = re.compile(re.escape(where), re.IGNORECASE)
        return {
            "$or": [
                {"username": regex},
                {"email": regex},
            ],
        }

    if isinstance(where, dict):
        return _resolve_where_dict(where)

    return {}


OPERATORS: dict[str, Callable[[str, Any], dict[str, Any]]] = {
    "eq": lambda field, value: {field: _check_value(value)},
    "neq": lambda field, value: {field: {"$ne": _check_value(value)}},
    "lt": lambda field, value: {field: {"$lt": value}},
    "gt": lambda field, value: {field: {"$gt": value}},
    "le": lambda field, value: {field: {"$lte": value}},
    "ge": lambda field, value: {field: {"$gte": value}},
    "in": lambda field, value: {
        field: {"$in": [_check_value(v) for v in value]},
    },
    "not_in": lambda field, value: {
        field: {"$nin": [_check_value(v) for v in value]},
    },
    "startswith": lambda field, value: {
        field: re.compile(f"^{re.escape(value)}", re.IGNORECASE),
    },
    "not_startswith": lambda field, value: {
        field: {"$not": re.compile(f"^{re.escape(value)}", re.IGNORECASE)},
    },
    "endswith": lambda field, value: {
        field: re.compile(f"{re.escape(value)}$", re.IGNORECASE),
    },
    "not_endswith": lambda field, value: {
        field: {"$not": re.compile(f"{re.escape(value)}$", re.IGNORECASE)},
    },
    "contains": lambda field, value: {
        field: re.compile(re.escape(value), re.IGNORECASE),
    },
    "not_contains": lambda field, value: {
        field: {"$not": re.compile(re.escape(value), re.IGNORECASE)},
    },
    "is_false": lambda field, value: {field: False},
    "is_true": lambda field, value: {field: True},
    "is_null": lambda field, value: {field: None},
    "is_not_null": lambda field, value: {field: {"$ne": None}},
    "between": lambda field, value: {
        field: {
            "$gte": value[0],
            "$lte": value[1],
        },
    },
    "not_between": lambda field, value: {
        "$or": [{field: {"$lt": value[0]}}, {field: {"$gt": value[1]}}],
    },
}


def _check_value(value: Any) -> Any:
    """Преобразует строки в ObjectId если это _id"""
    if isinstance(value, str) and ObjectId.is_valid(value):
        return ObjectId(value)
    return value


def _resolve_where_dict(
    where: dict[str, Any],
    current_field: str | None = None,
) -> dict[str, Any]:
    """Рекурсивно преобразует where dict в MongoDB filter"""
    queries = []

    for key, value in where.items():
        if key == "or":
            or_queries = [_resolve_where_dict(q) for q in value]
            queries.append({"$or": or_queries})

        elif key == "and":
            and_queries = [_resolve_where_dict(q) for q in value]
            queries.append({"$and": and_queries})

        elif key in OPERATORS:
            # Применяем оператор к текущему полю
            if current_field:
                queries.append(OPERATORS[key](current_field, value))
            else:
                raise InvalidQueryOperatorError(operator=key)

        else:
            # Это название поля, рекурсивно обрабатываем вложенные операторы
            queries.append(_resolve_where_dict(value, current_field=key))

    if len(queries) == 1:
        return queries[0]
    elif len(queries) > 1:
        return {"$and": queries}

    return {}
