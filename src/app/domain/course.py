from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class CourseStatus(str, Enum):
    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class CourseLevel(str, Enum):
    BEGINNER = "beginner"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"


@dataclass
class Lesson:
    title: str
    duration_minutes: int
    order: int
    video_url: str | None = None
    is_free: bool = False


@dataclass
class Course:
    _id: str | None = None
    title: str = ""
    description: str = ""
    slug: str = ""
    status: CourseStatus = CourseStatus.DRAFT
    level: CourseLevel = CourseLevel.BEGINNER
    price: float = 0.0
    instructor_name: str = ""
    tags: list[str] = field(default_factory=list)
    lessons: list[Lesson] = field(default_factory=list)
    is_published: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
