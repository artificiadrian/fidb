import enum
from sqlalchemy import Enum
from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import func


class PathType(enum.Enum):
    linux = "l"
    windows = "w"


class Base(DeclarativeBase):
    pass


class Path(Base):
    __tablename__ = "paths"
    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column()
    is_dir: Mapped[bool] = mapped_column()
    type: Mapped[PathType] = mapped_column(type_=Enum(PathType))
    added_at: Mapped[datetime] = mapped_column(server_default=func.now())


def to_path(path: str) -> Path:
    return Path(
        value=path,
        is_dir=path.endswith("/") or path.endswith("\\"),
        type=PathType.linux if "/" in path else PathType.windows,
    )
