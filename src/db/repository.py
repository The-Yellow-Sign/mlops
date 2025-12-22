import logging
from datetime import datetime
from typing import Any, AsyncIterator, Optional

from pydantic import ValidationError
from sqlalchemy import exc, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Author, Commit, File, Group, Project
from db.schemas import AuthorData, CommitData, FileData, GroupData, ProjectData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
    ],
)
logger = logging.getLogger(__name__)


class BaseRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def _handle_error(self, error: Exception, entity_name: str, entity_id: str = None):
        """Обработать ошибку и залогировать ее."""
        error_map = {
            ValidationError: (f"Validation error creating {entity_name}", ValueError),
            exc.IntegrityError: (f"Integrity error for {entity_name}", None),
            exc.MultipleResultsFound: (f"Multiple {entity_name}s found", ValueError),
            exc.SQLAlchemyError: (f"SQLAlchemy error for {entity_name}", None),
        }

        for error_type, (log_prefix, raise_type) in error_map.items():
            if isinstance(error, error_type):
                log_msg = f"{log_prefix}"
                if entity_id:
                    log_msg += f" with id {entity_id}"
                logger.error(f"{log_msg}: {error}")

                if raise_type:
                    raise raise_type(f"{log_msg}: {error}") from error
                raise

        log_msg = f"Unexpected error for {entity_name}"
        if entity_id:
            log_msg += f" with id {entity_id}"
        logger.error(f"{log_msg}: {error}")
        raise


class GroupRepository(BaseRepository):
    """Репозиторий данных Group."""

    async def get_group_by_gitlab_id(self, gitlab_id: str) -> Optional[Group]:
        """Найти группу по GitLab ID."""
        try:
            stmt = select(Group).where(Group.gitlab_id == gitlab_id)
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except Exception as e:
            self._handle_error(e, "group", gitlab_id)

    async def create_or_update_group(
        self, data: dict[str, Any], parent_id: Optional[int] = None
    ) -> Group:
        """Создать или обновить группу."""
        try:
            validated_data = GroupData.model_validate(data)
            gitlab_id = validated_data.id

            existing_group = await self.get_group_by_gitlab_id(gitlab_id)

            if existing_group:
                # TODO: Добавить логику обновления
                logger.info(f"Group {gitlab_id} already exists, skipping creation")
                return existing_group

            group = Group(
                gitlab_id=gitlab_id,
                name=validated_data.name,
                full_path=validated_data.fullPath,
                web_url=validated_data.webUrl,
                parent_id=parent_id,
            )
            self.session.add(group)
            await self.session.flush()
            logger.info(f"{gitlab_id} group created successfully")
            return group

        except Exception as e:
            self._handle_error(e, "group", gitlab_id)

    async def process_group(
        self, data: dict[str, Any], parent_id: Optional[int] = None
    ):
        """Обработать группу и ее подгруппы."""
        await self.create_or_update_group(data, parent_id)

        descendant_groups = data.get("descendantGroups", {}).get("nodes", [])
        for descendant_group in descendant_groups:
            parent_gitlab_id = descendant_group.get("parent", {}).get("id") or parent_id
            parent_group = await self.get_group_by_gitlab_id(parent_gitlab_id)
            parent_id = parent_group.id if parent_group else None
            await self.create_or_update_group(
                descendant_group, parent_id=parent_id
            )


class ProjectRepository(BaseRepository):
    """Репозиторий данных Project."""

    async def get_project_by_gitlab_id(self, gitlab_id: str) -> Optional[Project]:
        """Найти проект по GitLab ID."""
        try:
            stmt = select(Project).where(Project.gitlab_id == gitlab_id)
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except Exception as e:
            self._handle_error(e, "project", gitlab_id)

    async def create_or_update_project(
        self, data: dict[str, Any], group_id: Optional[int] = None
    ):
        """Создать или обновить проект."""

        try:
            validated_data = ProjectData.model_validate(data)
            gitlab_id = validated_data.id

            existing_project = await self.get_project_by_gitlab_id(gitlab_id)

            if existing_project:
                # TODO: Добавить логику обновления
                logger.info(f"Project {gitlab_id} already exists, skipping creation")
                return existing_project

            project = Project(
                gitlab_id=gitlab_id,
                name=validated_data.name,
                full_path=validated_data.fullPath,
                web_url=validated_data.webUrl,
                group_id=group_id,
            )
            self.session.add(project)
            await self.session.flush()
            logger.info(f"Project {gitlab_id} created successfully")
            return project

        except Exception as e:
            self._handle_error(e, "project", gitlab_id)

    async def iterate_all_projects(
        self, batch_size: int = 10
    ) -> AsyncIterator[list[Project]]:
        """Итерация по всем проектам БД батчами."""
        offset = 0
        while True:
            try:
                stmt = (
                    select(Project)
                    .order_by(Project.id)
                    .offset(offset)
                    .limit(batch_size)
                )
                result = await self.session.execute(stmt)
                projects = result.scalars().all()
            except Exception as e:
                self._handle_error(e, "projects")

            if not projects:
                break
            yield projects
            offset += batch_size


class FileRepository(BaseRepository):
    """Репозиторий данных File."""

    async def get_file_by_gitlab_id(self, gitlab_id: str) -> Optional[File]:
        """Найти файл по GitLab ID."""
        try:
            stmt = select(File).where(File.gitlab_id == gitlab_id)
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except Exception as e:
            self._handle_error(e, "file")

    async def create_file(
        self,
        data: dict[str, Any],
        project_id: int,
        last_commit_id: Optional[int] = None,
    ):
        """Создать или обновить файл."""
        try:
            validated_data = FileData.model_validate(data)
            gitlab_id = validated_data.id

            existing_file = await self.get_file_by_gitlab_id(gitlab_id)

            if existing_file:
                # TODO: Добавить логику обновления
                logger.info(f"File {gitlab_id} already exists, skipping creation")
                return existing_file

            file = File(
                gitlab_id=gitlab_id,
                project_id=project_id,
                name=validated_data.name,
                path=validated_data.path,
                web_url=validated_data.webUrl,
                # raw_url
                sha=validated_data.sha,
                # deleted,
                last_commit_id=last_commit_id,
            )
            self.session.add(file)
            await self.session.flush()
            logger.info(f"File {gitlab_id} created successfully")
            return file

        except Exception as e:
            self._handle_error(e, "file")

    async def update_file(
        self,
        file_id: int,
        commit_id: int = None,
        raw_file: str = None,
    ):
        try:
            stmt = select(File).where(File.id == file_id)
            result = await self.session.execute(stmt)
            file = result.scalar_one_or_none()

            if not file:
                logger.warning(f"File with id {file_id} not found for update")
                return None

            update_made = False
            if commit_id is not None:
                if file.last_commit_id == commit_id:
                    logger.info(
                        f"Commit {commit_id} is already linked to file {file_id}"
                    )
                    return None
                file.last_commit_id = commit_id
                logger.info(f"Linked commit {commit_id} to file {file_id}")
                update_made = True

            if raw_file is not None and file.content != raw_file:
                file.content = raw_file
                logger.info(f"Uploaded content for file {file_id}")
                update_made = True

            if update_made:
                await self.session.flush()
                return True
            else:
                logger.warning(f"No changes made to file {file_id}")
                return None

        except Exception as e:
            self._handle_error(e, "file")

    async def iterate_files_by_project(
        self, project_id: int, batch_size: int = 20
    ) -> AsyncIterator[list[File]]:
        """Итерация по файлам конкретного проекта батчами."""
        offset = 0
        while True:
            try:
                stmt = (
                    select(File)
                    .where(File.project_id == project_id)
                    .order_by(File.id)
                    .offset(offset)
                    .limit(batch_size)
                )
                result = await self.session.execute(stmt)
                files = result.scalars().all()

            except Exception as e:
                self._handle_error(e, "files")

            if not files:
                break
            yield files
            offset += batch_size


class CommitRepository(BaseRepository):
    """Репозиторий данных Commit."""

    async def get_commit_by_sha(self, sha: str) -> Optional[Commit]:
        """Найти коммит по sha."""
        try:
            stmt = select(Commit).where(Commit.sha == sha)
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except Exception as e:
            self._handle_error(e, "commit")

    async def create_commit(
        self, data: dict[str, Any], author_id: int
    ) -> Optional[Commit]:
        """Создать коммит."""

        try:
            validated_data = CommitData.model_validate(data)
            sha = validated_data.id

            existing_commit = await self.get_commit_by_sha(sha)

            if existing_commit:
                # TODO: Добавить логику обновления. Надо ли тут?
                logger.info(f"Commit with SHA {sha} already exists, skipping creation")
                return existing_commit

            timestamp_str = validated_data.created_at
            if timestamp_str:
                try:
                    parsed_timestamp = datetime.fromisoformat(
                        timestamp_str.replace("Z", "+00:00")
                    )
                    timestamp = parsed_timestamp.replace(tzinfo=None)
                except ValueError:
                    logger.warning(
                        f"Invalid timestamp format for commit {sha}: {timestamp_str}, using None"
                    )
                    timestamp = None
            else:
                logger.warning("Timestamp is empty, using None")
                timestamp = None

            commit = Commit(
                sha=sha,
                author_id=author_id,
                timestamp=timestamp,
                web_url=validated_data.web_url,
                title=validated_data.title,
            )
            self.session.add(commit)
            await self.session.flush()
            logger.info(f"Commit {sha} created successfully")
            return commit

        except Exception as e:
            self._handle_error(e, "commit")


class AuthorRepository(BaseRepository):
    """Репозиторий данных Author."""

    async def get_author_by_username(self, username: str) -> Optional[Author]:
        """Найти автора по username."""
        try:
            stmt = select(Author).where(Author.username == username)
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except Exception as e:
            self._handle_error(e, "author")

    async def create_author(self, data: dict[str, Any]):
        """Создать автора."""
        try:
            validated_data = AuthorData.model_validate(data)
            username = validated_data.author_name

            existing_author = await self.get_author_by_username(username)

            if existing_author:
                # TODO: Добавить логику обновления
                logger.info(f"Author {username} already exists, skipping creation.")
                return existing_author

            author = Author(username=username, email=validated_data.author_email)
            self.session.add(author)
            await self.session.flush()
            logger.info(f"Author {username} created successfully")
            return author

        except Exception as e:
            self._handle_error(e, "author")
