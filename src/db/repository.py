from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Group, Project


class GroupRepository:
    """Репозиторий данных Group."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_group_by_gitlab_id(self, gitlab_id: str) -> Optional[Group]:
        """Найти группу по GitLab ID."""
        stmt = select(Group).where(Group.gitlab_id == gitlab_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create_or_update_group(
        self, data: dict[str, Any], parent_id: Optional[int] = None
    ):
        """Создать или обновить группу."""
        gitlab_id = data["id"]

        existing_group = await self.get_group_by_gitlab_id(gitlab_id)

        if existing_group:
            # TODO: Добавить логику обновления
            return existing_group

        group = Group(
            gitlab_id=gitlab_id,
            name=data.get("name", ""),
            full_path=data.get("fullPath", ""),
            web_url=data.get("webUrl", ""),
            parent_id=parent_id,
        )
        self.session.add(group)
        await self.session.flush()

    async def process_group(
        self, data: dict[str, Any], parent_id: Optional[int] = None
    ):
        """Обработать группу и ее подгруппы."""
        await self.create_or_update_group(data, parent_id)

        descendant_groups = data.get("descendantGroups", {}).get("nodes", [])
        for descendant_group in descendant_groups:
            parent_id = descendant_group.get("parent", {}).get("id") or parent_id
            await self.create_or_update_group(descendant_group, parent_id=parent_id)


class ProjectRepository:
    """Репозиторий данных Group."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_project_by_gitlab_id(self, gitlab_id: str) -> Optional[Project]:
        """Найти проект по GitLab ID."""
        stmt = select(Project).where(Project.gitlab_id == gitlab_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create_or_update_project(
        self, data: dict[str, Any], group_id: Optional[int] = None
    ):
        """Создать или обновить проект."""
        gitlab_id = data["id"]

        existing_project = await self.get_project_by_gitlab_id(gitlab_id)

        if existing_project:
            # TODO: Добавить логику обновления
            return existing_project

        project = Project(
            gitlab_id=gitlab_id,
            name=data.get("name", ""),
            full_path=data.get("fullPath", ""),
            web_url=data.get("webUrl", ""),
            group_id=group_id,
        )
        self.session.add(project)
        await self.session.flush()
