from dataclasses import dataclass
from typing import Any

from adaptix import Retort
from starlette.requests import Request
from starlette_admin import ListField, RequestAction

from app.domain.course import Lesson


@dataclass(init=False)
class LessonsListField(ListField):
    retort = Retort()

    async def serialize_value(
            self, request: Request, value: Any, action: RequestAction,
    ) -> Any:
        if action == RequestAction.LIST:
            if not value:
                return "📚 Нет уроков"

            lesson_count = len(value)
            total_duration = sum(
                lesson.duration_minutes
                for lesson in value
            )
            free_count = sum(
                1 for lesson in value
                if lesson.is_free
            )

            parts = [f"📚 {lesson_count} урок(ов)"]
            parts.append(f"⏱️ {total_duration} мин")
            if free_count:
                parts.append(f"🎁 {free_count} бесплатных")

            return " · ".join(parts)

        if value:
            return self.retort.dump(value, list[Lesson])

        return await super().serialize_value(request, value, action)
