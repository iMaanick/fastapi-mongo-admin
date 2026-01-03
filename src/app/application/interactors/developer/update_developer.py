import logging
from dataclasses import dataclass
from typing import Any

from app.application.developer_repo import DeveloperRepository
from app.domain.developer import Address, Developer, Skill
from app.infrastructure.trackers.mongo_session import MongoSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class UpdateDeveloperRequest:
    developer_id: str
    username: str | None = None
    full_name: str | None = None
    city: str | None = None
    country: str | None = None
    coordinates: dict[str, float] | None = None
    languages: list[str] | None = None
    tags: set[str] | None = None
    skills: list[dict[str, Any]] | None = None
    projects: list[dict[str, Any]] | None = None
    metadata: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class UpdateDeveloperInteractor:
    developer_repository: DeveloperRepository
    session: MongoSession

    async def __call__(
        self,
        request_data: UpdateDeveloperRequest,
    ) -> Developer | None:
        logger.info("Updating developer: %s", request_data.developer_id)

        # Get existing developer
        developer = await self.developer_repository.get_by_id(
            request_data.developer_id,
        )

        if developer is None:
            logger.info("Developer not found: %s", request_data.developer_id)
            return None

        # Update fields if provided
        if request_data.username is not None:
            developer.username = request_data.username

        if request_data.full_name is not None:
            developer.full_name = request_data.full_name

        # Update address
        if any([request_data.city, request_data.country, request_data.coordinates]):
            if developer.address is None:
                developer.address = Address(
                    city=request_data.city or "",
                    country=request_data.country or "",
                    coordinates=request_data.coordinates or {},
                )
            else:
                if request_data.city is not None:
                    developer.address.city = request_data.city
                if request_data.country is not None:
                    developer.address.country = request_data.country
                if request_data.coordinates is not None:
                    developer.address.coordinates = request_data.coordinates

        if request_data.languages is not None:
            developer.languages = request_data.languages

        if request_data.tags is not None:
            developer.tags = request_data.tags

        # Update skills
        if request_data.skills is not None:
            developer.skills = [
                Skill(
                    name=skill_data["name"],
                    level=skill_data["level"],
                    years=skill_data["years"],
                )
                for skill_data in request_data.skills
            ]

        if request_data.projects is not None:
            developer.projects = request_data.projects

        if request_data.metadata is not None:
            developer.metadata = request_data.metadata

        await self.session.commit()

        logger.info(
            "Developer updated: %s (ID: %s)",
            developer.username,
            developer._id,
        )

        return developer
