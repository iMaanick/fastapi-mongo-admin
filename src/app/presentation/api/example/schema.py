from pydantic import BaseModel

from app.domain.model import Tag


class UpdateUserBody(BaseModel):
    username: str | None = None
    email: str | None = None
    tags: list[Tag] | None = None
    is_active: bool | None = None
