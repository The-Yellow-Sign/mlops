import asyncio
import logging
import time
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
from dotenv import load_dotenv
from pydantic import ValidationError
from sqlalchemy import exc
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import File, Project
from db.repository import (
    AuthorRepository,
    CommitRepository,
    FileRepository,
    ProjectRepository,
)
from db.schemas import GitLabCommitData, GitLabConfig

load_dotenv()

logger = logging.getLogger(__name__)


class GitLabCommitCollector:
    """Сборщик информации о коммитах из GitLab API."""

    def __init__(self, config: GitLabConfig):
        if not config.gitlab_token:
            logger.error("GITLAB_TOKEN is required")
            raise ValueError("GITLAB_TOKEN is required")

        self.config = config
        self.headers = {
            "Authorization": f"Bearer {config.gitlab_token}",
            "Content-Type": "application/json",
        }

    def _encode_project_path(self, project_path: str) -> str:
        return quote(project_path, safe="")

    async def _make_gitlab_request(self, url: str, params: dict) -> Optional[Any]:
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)

        for attempt in range(self.config.max_retries):
            try:
                async with aiohttp.ClientSession(
                    headers=self.headers, timeout=timeout
                ) as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            return await response.json()

                        if response.status == 429:
                            ra = response.headers.get("Retry-After")
                            try:
                                retry_after = (
                                    int(ra)
                                    if ra is not None
                                    else int(self.config.retry_delay)
                                )
                            except ValueError:
                                retry_after = int(self.config.retry_delay)

                            logger.warning(
                                f"Rate limited by GitLab API. Retry after {retry_after}s"
                            )
                            await asyncio.sleep(retry_after * (attempt + 1))
                            continue

                        if response.status in (500, 502, 503, 504):
                            body = await response.text()
                            logger.warning(
                                f"GitLab temporary error {response.status} on attempt "
                                f"{attempt + 1}/{self.config.max_retries}: {body}"
                            )
                            if attempt < self.config.max_retries - 1:
                                await asyncio.sleep(
                                    self.config.retry_delay * (2**attempt)
                                )
                                continue
                            return None

                        if response.status in (401, 403):
                            body = await response.text()
                            raise PermissionError(
                                f"GitLab auth error {response.status}: {body}"
                            )

                        if response.status == 404:
                            body = await response.text()
                            logger.warning(
                                f"GitLab 404 for url={url} params={params}: {body}"
                            )
                            return None

                        body = await response.text()
                        logger.error(f"Error {response.status} from GitLab API: {body}")
                        return None

            except aiohttp.ClientError as e:
                logger.warning(
                    f"Network error on attempt {attempt + 1}/{self.config.max_retries}: {e}"
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (2**attempt))
                    continue
                else:
                    logger.error(f"Max retries exceeded for GitLab API request: {e}")
                    return None
            except asyncio.TimeoutError:
                logger.warning(
                    f"Timeout on attempt {attempt + 1}/{self.config.max_retries}"
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (2**attempt))
                    continue
                else:
                    logger.error("Max retries exceeded due to timeouts")
                    return None

        return None

    async def get_commit_for_file(
        self, project: Project, file: File
    ) -> Optional[GitLabCommitData]:
        """Получить информацию о последнем коммите для файла."""

        try:
            project_path = project.full_path
            file_path = file.path
            encoded_project_path = self._encode_project_path(project_path)
            url = f"{self.config.rest_url}/{encoded_project_path}/repository/commits"

            params = {
                "path": file_path,
                "page": 1,
                "per_page": 1,
            }

            commits = await self._make_gitlab_request(url, params)
            if not commits:
                logger.warning(
                    f"No commits found for file {file_path} (ID: {file.id}) in project {project_path} (ID: {project.id})"
                )
                return None

            if not isinstance(commits, list) or len(commits) == 0:
                logger.warning(f"Unexpected response format for commits: {commits}")
                return None

            return GitLabCommitData.model_validate(commits[0])

        except PermissionError as e:
            logger.error(
                f"Permission error fetching commit for {file_path} (ID: {file.id}) "
                f"in project {project_path} (ID: {project.id}): {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error fetching commit for {file_path} (ID: {file.id}) "
                f"in project {project_path} (ID: {project.id}): {e}"
            )
            return None

    async def process_data(self, db_session: AsyncSession, run_id: int):
        file_repo = FileRepository(db_session)
        project_repo = ProjectRepository(db_session)

        processed_ok = 0
        processed_failed = 0
        processed_skipped = 0
        start_time = time.time()

        try:
            async for projects_batch in project_repo.iterate_projects_by_run(
                batch_size=4, run_id=run_id
            ):
                for project in projects_batch:
                    logger.info(
                        f"Commit processing project: {project.full_path} (ID: {project.id})"
                    )

                    async for files_batch in file_repo.iterate_files_by_project(
                        project.id, batch_size=7
                    ):
                        for file in files_batch:
                            try:
                                async with db_session.begin_nested():
                                    result = await self._process_single_file(
                                        file, project, db_session
                                    )
                                    if result is True:
                                        processed_ok += 1
                                    elif result is False:
                                        processed_failed += 1
                                    else:  # None
                                        processed_skipped += 1

                            except PermissionError:
                                raise
                            except Exception as e:
                                processed_failed += 1
                                logger.error(
                                    f"Error processing file {file.path} (ID: {file.id}) "
                                    f"of project {project.full_path} (ID: {project.id}): {e}"
                                )

                        try:
                            await db_session.commit()
                            logger.debug(f"Committed batch of {len(files_batch)} files")
                        except exc.SQLAlchemyError as e:
                            logger.error(f"Database commit error: {e}")
                            await db_session.rollback()
                            raise

            total_time = time.time() - start_time
            logger.info(
                f"Commit collector finished: ok={processed_ok}, failed={processed_failed}, "
                f"skipped={processed_skipped}, time={total_time:.2f}s"
            )

        except Exception as e:
            logger.error(f"Critical error during commit processing: {e}")
            await db_session.rollback()
            raise

    async def _process_single_file(
        self, file: File, project: Project, db_session: AsyncSession
    ) -> bool:

        commit_data = await self.get_commit_for_file(project, file)
        if not commit_data:
            logger.warning(
                f"No commit data found for file: {file.path} (ID: {file.id}) "
                f"of project {project.full_path} (ID: {project.id})"
            )
            return None

        try:
            return await self._save_data(
                commit_data.model_dump(), file, project, db_session
            )
        except Exception as e:
            logger.error(
                f"Error saving commit data for file {file.path} (ID: {file.id}) "
                f"of project {project.full_path} (ID: {project.id}): {e}"
            )
            return False

    async def _save_data(
        self,
        data: dict[str, Any],
        file: File,
        project: Project,
        db_session: AsyncSession,
    ) -> bool:
        """Сохранить данные о коммите в БД с обработкой ошибок."""
        if not data:
            logger.warning("Attempt to save empty commit data")
            return False

        commit_repo = CommitRepository(db_session)
        author_repo = AuthorRepository(db_session)
        file_repo = FileRepository(db_session)

        try:
            await author_repo.create_author(data)

            author_entry = await author_repo.get_author_by_username(
                data.get("author_name", "")
            )
            if not author_entry:
                logger.error(
                    f"Failed to retrieve or create author: {data.get('author_name', 'unknown')}"
                )
                return False

            created_commit = await commit_repo.create_commit(
                data=data, author_id=author_entry.id
            )
            if not created_commit:
                logger.error(
                    f"Failed to create or find commit for file {file.id} (ID: {file.id})"
                )
                return False

            commit_id = created_commit.id
            updated = await file_repo.update_file(file_id=file.id, commit_id=commit_id)
            if updated:
                logger.info(
                    f"Successfully linked commit ID {commit_id} to file {file.id} (ID: {file.id}) "
                    f"of project {project.full_path} (ID: {project.id})"
                )
                return True
            return None

        except (ValidationError, exc.SQLAlchemyError) as e:
            logger.error(
                f"Database/validation error saving commit data for file {file.id} (ID: {file.id}) "
                f"of project {project.full_path} (ID: {project.id}): {e}"
            )
            return False

        except Exception as e:
            logger.error(
                f"Unexpected error saving commit data for file {file.id} (ID: {file.id}) "
                f"of project {project.full_path} (ID: {project.id}): {e}"
            )
            return False


# TODO: исправить счетчик processed_failed
