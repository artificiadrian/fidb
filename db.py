import enum
from sqlalchemy import Enum
from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import func


class PathType(enum.Enum):
    linux = "lin"
    windows = "win"


class Base(DeclarativeBase):
    pass


class FiPath(Base):
    __tablename__ = "paths"
    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column()
    is_dir: Mapped[bool] = mapped_column()
    type: Mapped[PathType] = mapped_column(type_=Enum(PathType))
    added_at: Mapped[datetime] = mapped_column(server_default=func.now())


def to_path(path: str) -> FiPath:
    path = path.strip()
    obj = FiPath(
        value=path,
        is_dir=path.endswith("/") or path.endswith("\\"),
        type=PathType.linux.value if "/" in path else PathType.windows.value,
    )

    if obj.is_dir:
        obj.value = obj.value.rstrip("/\\")

    return obj
