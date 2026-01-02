from dataclasses import dataclass, field
from typing import Any


@dataclass
class Address:
    """Nested model for address"""

    city: str
    country: str
    coordinates: dict[str, float] = field(default_factory=dict)


@dataclass
class Skill:
    """Nested model for skills"""

    name: str
    level: str
    years: int


@dataclass
class Developer:
    """Main model with various nested structures for testing"""

    username: str
    full_name: str
    _id: str | None = None

    # Nested model
    address: Address | None = None

    # List of strings
    languages: list[str] = field(default_factory=list)

    # Set
    tags: set[str] = field(default_factory=set)

    # List of nested models
    skills: list[Skill] = field(default_factory=list)

    # List of dicts
    projects: list[dict[str, Any]] = field(default_factory=list)

    # Nested dict
    metadata: dict[str, Any] = field(default_factory=dict)
