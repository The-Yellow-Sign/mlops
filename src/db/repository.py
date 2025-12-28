import logging
from datetime import datetime
from typing import Any, AsyncIterator, Optional

from pydantic import ValidationError
from sqlalchemy import exc, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Author, CollectionRun, Commit, File, Group, Project
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
                else:
                    raise

        log_msg = f"Unexpected error for {entity_name}"
        if entity_id:
            log_msg += f" with id {entity_id}"
        logger.error(f"{log_msg}: {error}")
        raise


class RunRepository(BaseRepository):
    async def close_all_active_runs(self):
        try:
            stmt = (
                update(CollectionRun)
                .where(CollectionRun.status == "active")
                .values(status="closed")
            )
            await self.session.execute(stmt)
            await self.session.flush()
        except exc.SQLAlchemyError as e:
            self._handle_error(e, "collection_runs")

    async def get_active_run_by_full_path(
        self, full_path: str
    ) -> Optional[CollectionRun]:
        try:
            stmt = (
                select(CollectionRun)
                .where(
                    CollectionRun.status == "active",
                    CollectionRun.full_path == full_path,
                )
                .order_by(CollectionRun.id.desc())
            )
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except exc.SQLAlchemyError as e:
            self._handle_error(e, "collection_run")

    async def start_new_active_run(self, full_path: str) -> CollectionRun:
        try:
            await self.close_all_active_runs()
            run = CollectionRun(full_path=full_path, status="active")
            self.session.add(run)
            await self.session.flush()
            return run
        except exc.SQLAlchemyError as e:
            self._handle_error(e, "collection_run")

    async def get_or_create_active_run(self, full_path: str) -> CollectionRun:
        existing = await self.get_active_run_by_full_path(full_path)
        if existing:
            return existing
        return await self.start_new_active_run(full_path)


class GroupRepository(BaseRepository):
    """Репозиторий данных Group."""

    async def get_group_by_gitlab_id(self, gitlab_id: str) -> Optional[Group]:
        """Найти группу по GitLab ID."""
        try:
            stmt = select(Group).where(Group.gitlab_id == gitlab_id)
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except exc.SQLAlchemyError as e:
            self._handle_error(e, "group", gitlab_id)

    async def create_or_update_group(
        self, data: dict[str, Any], parent_id: Optional[int] = None
    ) -> Group:
        """Создать или обновить группу."""
        gitlab_id: str | None = None
        try:
            validated_data = GroupData.model_validate(data)
            gitlab_id = validated_data.id

            existing_group = await self.get_group_by_gitlab_id(gitlab_id)

            if existing_group:
                updated = await self._update_group_if_needed(
                    existing_group, validated_data, parent_id
                )
                if updated:
                    await self.session.flush()
                    logger.info(f"Group {validated_data.fullPath} updated successfully")
                else:
                    logger.info(f"Group {validated_data.fullPath} already up-to-date")
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

        except (ValidationError, exc.SQLAlchemyError) as e:
            self._handle_error(e, "group", gitlab_id)

    async def _update_group_if_needed(
        self,
        group: Group,
        validated_data: GroupData,
        parent_id: Optional[int] = None,
    ) -> bool:
        """Обновить группу, если данные изменились.

        Returns:
            bool: True если были внесены изменения, иначе False

        """
        changes = {}

        if group.name != validated_data.name:
            changes["name"] = validated_data.name
            group.name = validated_data.name

        if group.full_path != validated_data.fullPath:
            changes["full_path"] = validated_data.fullPath
            group.full_path = validated_data.fullPath

        if group.web_url != validated_data.webUrl:
            changes["web_url"] = validated_data.webUrl
            group.web_url = validated_data.webUrl

        if parent_id is not None and group.parent_id != parent_id:
            changes["parent_id"] = parent_id
            group.parent_id = parent_id

        if changes:
            logger.debug(
                f"Group {group.full_path} (ID: {group.id}) changed fields: {list(changes.keys())}"
            )
            return True

        return False

    async def process_group(
        self, data: dict[str, Any], parent_id: Optional[int] = None
    ):
        """Обработать группу и ее подгруппы."""
        await self.create_or_update_group(data, parent_id)

        descendant_groups = data.get("descendantGroups", {}).get("nodes", [])
        for descendant_group in descendant_groups:
            parent_gitlab_id = descendant_group.get("parent", {}).get("id")
            if parent_gitlab_id:
                parent_group = await self.get_group_by_gitlab_id(parent_gitlab_id)
                descendant_parent_id = parent_group.id if parent_group else None
            else:
                descendant_parent_id = None

            await self.create_or_update_group(
                descendant_group, parent_id=descendant_parent_id
            )


class ProjectRepository(BaseRepository):
    """Репозиторий данных Project."""

    async def get_project_by_gitlab_id(self, gitlab_id: str) -> Optional[Project]:
        """Найти проект по GitLab ID."""
        try:
            stmt = select(Project).where(Project.gitlab_id == gitlab_id)
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except exc.SQLAlchemyError as e:
            self._handle_error(e, "project", gitlab_id)

    async def create_or_update_project(
        self,
        data: dict[str, Any],
        group_id: Optional[int] = None,
        run_id: Optional[int] = None,
    ):
        """Создать или обновить проект."""
        gitlab_id: str | None = None
        try:
            validated_data = ProjectData.model_validate(data)
            gitlab_id = validated_data.id
            if run_id is None:
                raise ValueError("run_id is required to save project in current run")

            existing_project = await self.get_project_by_gitlab_id(gitlab_id)

            if existing_project:
                updated = await self._update_project_if_needed(
                    existing_project, validated_data, group_id
                )

                if existing_project.run_id != run_id:
                    existing_project.run_id = run_id

                if updated:
                    await self.session.flush()
                    logger.info(
                        f"Project {validated_data.fullPath} updated successfully"
                    )
                else:
                    logger.info(f"Project {validated_data.fullPath} already up-to-date")
                return existing_project

            project = Project(
                gitlab_id=gitlab_id,
                name=validated_data.name,
                full_path=validated_data.fullPath,
                web_url=validated_data.webUrl,
                group_id=group_id,
                run_id=run_id,
            )
            self.session.add(project)
            await self.session.flush()
            logger.info(
                f"Project {project.full_path} (ID: {project.id}) created successfully"
            )
            return project

        except (ValidationError, exc.SQLAlchemyError) as e:
            self._handle_error(e, "project", gitlab_id)

    async def _update_project_if_needed(
        self,
        project: Project,
        validated_data: ProjectData,
        group_id: Optional[int] = None,
    ) -> bool:
        """Обновить проект, если данные изменились.

        Returns:
            bool: True если были внесены изменения, иначе False

        """
        changes = {}

        if project.name != validated_data.name:
            changes["name"] = validated_data.name
            project.name = validated_data.name

        if project.full_path != validated_data.fullPath:
            changes["full_path"] = validated_data.fullPath
            project.full_path = validated_data.fullPath

        if project.web_url != validated_data.webUrl:
            changes["web_url"] = validated_data.webUrl
            project.web_url = validated_data.webUrl

        if group_id is not None and project.group_id != group_id:
            changes["group_id"] = group_id
            project.group_id = group_id

        if changes:
            logger.debug(
                f"Project {project.full_path} (ID: {project.id}) changed fields: {list(changes.keys())}"
            )
            return True

        return False

    async def iterate_projects_by_run(
        self, run_id: int, batch_size: int = 10
    ) -> AsyncIterator[list[Project]]:
        """Итерация по всем проектам БД батчами."""
        offset = 0
        while True:
            try:
                stmt = (
                    select(Project)
                    .where(Project.run_id == run_id)
                    .order_by(Project.id)
                    .offset(offset)
                    .limit(batch_size)
                )
                result = await self.session.execute(stmt)
                projects = result.scalars().all()
            except exc.SQLAlchemyError as e:
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

        except exc.SQLAlchemyError as e:
            self._handle_error(e, "file")

    async def create_file(
        self,
        data: dict[str, Any],
        project_id: int,
        last_commit_id: Optional[int] = None,
    ):
        """Создать или обновить файл."""
        gitlab_id: str | None = None
        try:
            validated_data = FileData.model_validate(data)
            gitlab_id = validated_data.id

            existing_file = await self.get_file_by_gitlab_id(gitlab_id)

            if existing_file:
                updated = await self._update_file_if_needed(
                    existing_file, validated_data, project_id, last_commit_id
                )
                if updated:
                    await self.session.flush()
                    logger.info(f"File {validated_data.path} updated successfully")
                else:
                    logger.info(f"File {validated_data.path} already up-to-date")
                return existing_file

            file = File(
                gitlab_id=gitlab_id,
                project_id=project_id,
                name=validated_data.name,
                path=validated_data.path,
                web_url=validated_data.webUrl,
                sha=validated_data.sha,
                # deleted,
                last_commit_id=last_commit_id,
            )
            self.session.add(file)
            await self.session.flush()
            logger.info(f"File {gitlab_id} created successfully")
            return file

        except (ValidationError, exc.SQLAlchemyError) as e:
            self._handle_error(e, "file")

    async def _update_file_if_needed(
        self,
        file: File,
        validated_data: FileData,
        project_id: int,
        last_commit_id: Optional[int] = None,
    ) -> bool:
        """Обновить файл, если данные изменились.

        Returns:
            bool: True если были внесены изменения, иначе False

        """
        changes = {}

        if file.name != validated_data.name:
            changes["name"] = validated_data.name
            file.name = validated_data.name

        if file.path != validated_data.path:
            changes["path"] = validated_data.path
            file.path = validated_data.path

        if file.web_url != validated_data.webUrl:
            changes["web_url"] = validated_data.webUrl
            file.web_url = validated_data.webUrl

        if file.sha != validated_data.sha:
            changes["sha"] = validated_data.sha
            file.sha = validated_data.sha

        if file.project_id != project_id:
            changes["project_id"] = project_id
            file.project_id = project_id

        if last_commit_id is not None and file.last_commit_id != last_commit_id:
            changes["last_commit_id"] = last_commit_id
            file.last_commit_id = last_commit_id

        if changes:
            logger.debug(
                f"File {file.name} (ID: {file.id}) changed fields: {list(changes.keys())}"
            )
            return True

        return False

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
                    return False
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
                logger.debug(f"No changes made to file {file_id}")
                return False

        except exc.SQLAlchemyError as e:
            self._handle_error(e, "file")

    async def iterate_files_by_project(
        self, project_id: int, batch_size: int = 20, only_missing_content: bool = False
    ) -> AsyncIterator[list[File]]:
        offset = 0
        while True:
            try:
                stmt = select(File).where(File.project_id == project_id)
                if only_missing_content:
                    stmt = stmt.where(File.content.is_(None))

                stmt = stmt.order_by(File.id).offset(offset).limit(batch_size)
                result = await self.session.execute(stmt)
                files = result.scalars().all()

            except exc.SQLAlchemyError as e:
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

        except exc.SQLAlchemyError as e:
            self._handle_error(e, "commit")

    async def create_commit(
        self, data: dict[str, Any], author_id: int
    ) -> Optional[Commit]:
        """Создать коммит."""
        sha: str | None = None
        try:
            validated_data = CommitData.model_validate(data)
            sha = validated_data.id

            existing_commit = await self.get_commit_by_sha(sha)

            if existing_commit:
                updated = await self._update_commit_if_needed(
                    existing_commit, validated_data, author_id
                )
                if updated:
                    await self.session.flush()
                    logger.info(f"Commit {sha} updated successfully")
                else:
                    logger.info(f"Commit {sha} already up-to-date")
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

        except (ValidationError, exc.SQLAlchemyError) as e:
            self._handle_error(e, "commit")

    async def _update_commit_if_needed(
        self,
        commit: Commit,
        validated_data: CommitData,
        author_id: int,
    ) -> bool:
        """Обновить коммит, если данные изменились.

        Returns:
            bool: True если были внесены изменения, иначе False

        """
        changes = {}

        if commit.author_id != author_id:
            changes["author_id"] = author_id
            commit.author_id = author_id

        if commit.web_url != validated_data.web_url:
            changes["web_url"] = validated_data.web_url
            commit.web_url = validated_data.web_url

        if commit.title != validated_data.title:
            changes["title"] = validated_data.title
            commit.title = validated_data.title

        timestamp_str = validated_data.created_at
        if timestamp_str:
            try:
                parsed_timestamp = datetime.fromisoformat(
                    timestamp_str.replace("Z", "+00:00")
                )
                new_timestamp = parsed_timestamp.replace(tzinfo=None)
                if commit.timestamp != new_timestamp:
                    changes["timestamp"] = new_timestamp
                    commit.timestamp = new_timestamp
            except ValueError:
                logger.warning(
                    f"Invalid timestamp format for commit {commit.sha}: {timestamp_str}, skipping timestamp update"
                )

        if changes:
            logger.debug(
                f"Commit {commit.sha} (ID: {commit.id}) changed fields: {list(changes.keys())}"
            )
            return True

        return False


class AuthorRepository(BaseRepository):
    """Репозиторий данных Author."""

    async def get_author_by_username(self, username: str) -> Optional[Author]:
        """Найти автора по username."""
        try:
            stmt = select(Author).where(Author.username == username)
            result = await self.session.execute(stmt)
            return result.scalar_one_or_none()

        except exc.SQLAlchemyError as e:
            self._handle_error(e, "author")

    async def create_author(self, data: dict[str, Any]):
        """Создать автора."""
        username: str | None = None
        try:
            validated_data = AuthorData.model_validate(data)
            username = validated_data.author_name

            existing_author = await self.get_author_by_username(username)

            if existing_author:
                updated = await self._update_author_if_needed(
                    existing_author, validated_data
                )
                if updated:
                    await self.session.flush()
                    logger.info(f"Author {username} updated successfully")
                else:
                    logger.info(f"Author {username} already up-to-date")
                return existing_author

            author = Author(username=username, email=validated_data.author_email)
            self.session.add(author)
            await self.session.flush()
            logger.info(f"Author {username} created successfully")
            return author

        except (ValidationError, exc.SQLAlchemyError) as e:
            self._handle_error(e, "author")

    async def _update_author_if_needed(
        self,
        author: Author,
        validated_data: AuthorData,
    ) -> bool:
        """Обновить автора, если данные изменились.

        Returns:
            bool: True если были внесены изменения, иначе False

        """
        changes = {}

        if author.email != validated_data.author_email:
            changes["email"] = validated_data.author_email
            author.email = validated_data.author_email

        if changes:
            logger.debug(
                f"Author {author.username} (ID: {author.id}) changed fields: {list(changes.keys())}"
            )
            return True

        return False
