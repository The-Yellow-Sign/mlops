from typing import Optional

from sqlalchemy import ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Group(Base):
    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    gitlab_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200))
    full_path: Mapped[str] = mapped_column(String(500))
    web_url: Mapped[str] = mapped_column(String(600))
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("groups.id"))

    # Relationships
    children: Mapped[list["Group"]] = relationship(
        back_populates="parent", remote_side=[parent_id], cascade="all, delete"
    )
    parent: Mapped[Optional["Group"]] = relationship(
        back_populates="children", remote_side=[id]
    )
    projects: Mapped[list["Project"]] = relationship(back_populates="group")


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    gitlab_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200))
    full_path: Mapped[str] = mapped_column(String(500))
    web_url: Mapped[str] = mapped_column(String(600))

    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id"))

    # Relationships
    group: Mapped[Group] = relationship(back_populates="projects")
