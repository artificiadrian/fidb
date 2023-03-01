import enum
from sqlalchemy import Enum, ForeignKey
from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, Session, Mapped, mapped_column, relationship
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
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    category: Mapped["Category"] = relationship("Category", back_populates="paths")
    

class Category(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    added_at: Mapped[datetime] = mapped_column(server_default=func.now())
    paths: Mapped[list[FiPath]] = relationship("FiPath", back_populates="category")

def to_path(path: str, category_id: int) -> FiPath:
    path = path.strip()
    obj = FiPath(
        value=path,
        is_dir=path.endswith("/") or path.endswith("\\"),
        type=PathType.linux.value if "/" in path else PathType.windows.value,
        category_id=category_id,
    )

    if obj.is_dir:
        obj.value = obj.value.rstrip("/\\")

    return obj

def get_category(session, name: str) -> Category:
    return session.query(Category).filter(Category.name == name).first()