from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CreateDeveloperRequestSchema(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "username": "alice_dev",
                    "full_name": "Alice Smith",
                    "city": "San Francisco",
                    "country": "USA",
                    "coordinates": {"lat": 37.7749, "lon": -122.4194},
                    "languages": ["Python", "Go", "JavaScript"],
                    "tags": ["backend", "api", "microservices"],
                    "skills": [
                        {"name": "Python", "level": "expert", "years": 5},
                        {"name": "FastAPI", "level": "advanced", "years": 3},
                        {"name": "MongoDB", "level": "advanced", "years": 4},
                    ],
                    "projects": [
                        {
                            "name": "E-commerce API",
                            "tech": ["Python", "FastAPI", "MongoDB"],
                            "role": "Backend Lead",
                        },
                        {
                            "name": "Microservices Platform",
                            "tech": ["Go", "Docker", "Kubernetes"],
                            "role": "Senior Developer",
                        },
                    ],
                    "metadata": {
                        "preferences": {
                            "theme": "dark",
                            "notifications": {"email": True, "push": False},
                        },
                        "statistics": {"commits": 1523, "pull_requests": 234},
                        "badges": ["top_contributor", "code_reviewer"],
                    },
                },
            ],
        },
    )

    username: str = Field(..., description="Unique username")
    full_name: str = Field(..., description="Full name of developer")
    city: str = Field(..., description="City name")
    country: str = Field(..., description="Country name")
    coordinates: dict[str, float] = Field(
        default_factory=dict,
        description="GPS coordinates",
    )
    languages: list[str] = Field(
        default_factory=list,
        description="Programming languages",
    )
    tags: set[str] = Field(default_factory=set, description="Developer tags")
    skills: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of skills with proficiency levels",
    )
    projects: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of projects",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata with nested structures",
    )


from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AddressSchema(BaseModel):
    """Nested model for address"""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "city": "San Francisco",
                    "country": "USA",
                    "coordinates": {"lat": 37.7749, "lon": -122.4194},
                }
            ]
        }
    )

    city: str
    country: str
    coordinates: dict[str, float]


class SkillSchema(BaseModel):
    """Nested model for skills"""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"name": "Python", "level": "expert", "years": 5}]
        }
    )

    name: str
    level: str
    years: int


class DeveloperSchema(BaseModel):
    """Main model with various nested structures"""

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "_id": "69581038f2db860793b48cb2",
                    "username": "alice_dev",
                    "full_name": "Alice Smith",
                    "address": {
                        "city": "San Francisco",
                        "country": "USA",
                        "coordinates": {"lat": 37.7749, "lon": -122.4194},
                    },
                    "languages": ["Python", "Go", "JavaScript"],
                    "tags": ["backend", "api", "microservices"],
                    "skills": [
                        {"name": "Python", "level": "expert", "years": 5},
                        {"name": "FastAPI", "level": "advanced", "years": 3},
                    ],
                    "projects": [
                        {
                            "name": "E-commerce API",
                            "tech": ["Python", "FastAPI", "MongoDB"],
                            "role": "Backend Lead",
                        }
                    ],
                    "metadata": {
                        "preferences": {"theme": "dark"},
                        "statistics": {"commits": 1523},
                    },
                }
            ]
        },
    )

    username: str
    full_name: str
    developer_id: str | None = Field(None, alias="_id")
    address: AddressSchema | None = None
    languages: list[str]
    tags: list[str]  # set -> list for JSON serialization
    skills: list[SkillSchema]
    projects: list[dict[str, Any]]
    metadata: dict[str, Any]
