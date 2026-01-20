from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(AsyncAttrs, DeclarativeBase):

    """Базовый декларативный класс для всех моделей SQLAlchemy с поддержкой асинхронности."""

    pass


class CollectionRun(Base):

    """Модель запуска процесса сбора данных с отслеживанием статуса и времени."""

    __tablename__ = "collection_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now, nullable=False
    )

    full_path: Mapped[str] = mapped_column(String(500), nullable=False)

    status: Mapped[str] = mapped_column(
        String(32), default="active", nullable=False
    )

    projects: Mapped[list["Project"]] = relationship(back_populates="run")


class Group(Base):

    """Модель группы GitLab с поддержкой иерархической вложенности (parent/children)."""

    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    gitlab_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200))
    full_path: Mapped[str] = mapped_column(String(500))
    web_url: Mapped[str] = mapped_column(String(1000))

    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("groups.id"))

    # Relationships
    children: Mapped[list["Group"]] = relationship(
        back_populates="parent", cascade="all, delete"
    )
    parent: Mapped[Optional["Group"]] = relationship(
        back_populates="children", remote_side=[id]
    )
    projects: Mapped[list["Project"]] = relationship(back_populates="group")


class Project(Base):

    """Модель проекта GitLab, привязанная к группе и конкретному запуску сбора данных."""

    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    gitlab_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200))
    full_path: Mapped[str] = mapped_column(String(500))
    web_url: Mapped[str] = mapped_column(String(1000))

    group_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("groups.id"), nullable=True
    )
    run_id: Mapped[int] = mapped_column(ForeignKey("collection_runs.id"), nullable=False)

    # Relationships
    run: Mapped["CollectionRun"] = relationship(back_populates="projects")
    group: Mapped[Group] = relationship(back_populates="projects")

    files: Mapped[list["File"]] = relationship(back_populates="project")


class File(Base):

    """Модель файла репозитория, содержащая метаданные, контент и ссылку на последний коммит."""

    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    gitlab_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"))
    name: Mapped[str] = mapped_column(String(200))
    path: Mapped[str] = mapped_column(String(500))
    web_url: Mapped[str] = mapped_column(String(1000))
    content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sha: Mapped[str] = mapped_column(String(1000))
    # deleted: Mapped[bool] = mapped_column(Boolean)

    last_commit_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("commits.id"), nullable=True
    )

    # Relationships
    project: Mapped[Project] = relationship(back_populates="files")
    last_commit: Mapped[Optional["Commit"]] = relationship(back_populates="files")


class Commit(Base):

    """Модель Git-коммита, хранящая хеш, дату, сообщение и ссылку на автора."""

    __tablename__ = "commits"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sha: Mapped[str] = mapped_column(String(1000))
    author_id: Mapped[int] = mapped_column(ForeignKey("authors.id"))
    timestamp: Mapped[datetime] = mapped_column(DateTime)
    web_url: Mapped[str] = mapped_column(String(1000))
    title: Mapped[str] = mapped_column(String(1000))

    # Relationships
    files: Mapped[list[File]] = relationship(back_populates="last_commit")
    author: Mapped["Author"] = relationship(back_populates="commits")


class Author(Base):

    """Модель автора коммитов с информацией об имени и электронной почте."""

    __tablename__ = "authors"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(200))
    email: Mapped[Optional[str]] = mapped_column(String(200))

    # Relationships
    commits: Mapped[list[Commit]] = relationship(back_populates="author")
