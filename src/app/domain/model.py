from dataclasses import dataclass, field


@dataclass
class Tag:
    name: str


@dataclass
class User:
    username: str
    email: str
    tags: list[Tag] = field(default_factory=list)
    is_active: bool = True
    _id: str | None = None
    unused_field: str = field(init=False, default="")

    def __post_init__(self) -> None:
        self.unused_field = self.username + ": " + self.email
