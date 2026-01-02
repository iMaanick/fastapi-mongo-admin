import logging
from dataclasses import dataclass, field
from pprint import pprint
from typing import Any

from app.application.developer_repo import DeveloperRepository
from app.domain.developer import Address, Skill, Developer
from app.infrastructure.trackers.mongo_session import MongoSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class CreateDeveloperRequest:
    username: str
    full_name: str
    city: str
    country: str
    coordinates: dict[str, float] = field(default_factory=dict)
    languages: list[str] = field(default_factory=list)
    tags: set[str] = field(default_factory=set)
    skills: list[dict[str, Any]] = field(
        default_factory=list
    )  # [{"name": "Python", "level": "expert", "years": 5}]
    projects: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class CreateDeveloperInteractor:
    developer_repository: DeveloperRepository
    session: MongoSession

    async def __call__(
        self,
        request_data: CreateDeveloperRequest,
    ) -> Developer:
        logger.info("Creating developer: %s", request_data.username)

        # Build address
        address = Address(
            city=request_data.city,
            country=request_data.country,
            coordinates=request_data.coordinates,
        )

        # Build skills from dicts
        skills = [
            Skill(
                name=skill_data["name"],
                level=skill_data["level"],
                years=skill_data["years"],
            )
            for skill_data in request_data.skills
        ]

        # Create developer
        developer = Developer(
            username=request_data.username,
            full_name=request_data.full_name,
            address=address,
            languages=request_data.languages,
            tags=request_data.tags,
            skills=skills,
            projects=request_data.projects,
            metadata=request_data.metadata,
        )

        await self.developer_repository.add(developer)
        await self.session.commit()

        logger.info(
            "Developer created: %s with ID: %s", developer.username, developer._id
        )
        pprint(developer)
        return developer
