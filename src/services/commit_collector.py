import asyncio
import logging
import time
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import exc
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository import (
    AuthorRepository,
    CommitRepository,
    FileRepository,
    ProjectRepository,
)
from db.schemas import GitLabCommitData, GitLabConfig

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log")
    ]
)

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

    async def _make_gitlab_request(
        self, url: str, params: dict
    ) -> Optional[dict[str, Any]]:
        for attempt in range(self.config.max_retries):
            try:
                async with aiohttp.ClientSession(headers=self.headers) as session:
                    async with session.get(
                        url,
                        params=params,
                        timeout=aiohttp.ClientTimeout(
                            total=self.config.request_timeout
                        ),
                    ) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            retry_after = int(
                                response.headers.get(
                                    "Retry-After", self.config.retry_delay
                                )
                            )
                            logger.warning(
                                f"Rate limited by GitLab API. Retry after {retry_after}s"
                            )
                            await asyncio.sleep(retry_after * (attempt + 1))
                        else:
                            error_text = await response.text()
                            logger.error(
                                f"Error {response.status} from GitLab API: {error_text}"
                            )
                            return None

            except aiohttp.ClientError as e:
                logger.warning(
                    f"Network error on attempt {attempt + 1}/{self.config.max_retries}: {e}"
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (2**attempt))
                else:
                    logger.error(f"Max retries exceeded for GitLab API request: {e}")
                    return None
            except asyncio.TimeoutError:
                logger.warning(
                    f"Timeout on attempt {attempt + 1}/{self.config.max_retries}"
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (2**attempt))
                else:
                    logger.error("Max retries exceeded due to timeouts")
                    return None

        return None

    async def get_commit_for_file(
        self, project_path: str, file_path: str
    ) -> Optional[dict[str, Any]]:
        """Получить информацию о последнем коммите для файла."""
        encoded_project_path = self._encode_project_path(project_path)
        url = f"{self.config.rest_url}/{encoded_project_path}/repository/commits"

        params = {
            "path": file_path,
            "page": 1,
            "per_page": 1,
        }

        try:
            commits = await self._make_gitlab_request(url, params)
            if not commits:
                logger.warning(
                    f"No commits found for file {file_path} in project {project_path}"
                )
                return None

            if not isinstance(commits, list) or len(commits) == 0:
                logger.warning(f"Unexpected response format for commits: {commits}")
                return None

            return GitLabCommitData.model_validate(commits[0])

        except Exception as e:
            logger.error(
                f"Unexpected error fetching commit for {file_path} in project {project_path}: {e}"
            )
            return None

    async def process_data(self, db_session: AsyncSession):
        """Обработать данные с возможностью частичного сохранения прогресса."""
        file_repo = FileRepository(db_session)
        project_repo = ProjectRepository(db_session)

        processed_files = 0
        start_time = time.time()

        try:
            async for projects_batch in project_repo.iterate_all_projects(batch_size=4):
                for project in projects_batch:
                    logger.info(
                        f"Processing project: {project.full_path} (ID: {project.id})"
                    )

                    async for files_batch in file_repo.iterate_files_by_project(
                        project.id, batch_size=7
                    ):
                        for file in files_batch:
                            try:
                                await self._process_single_file(
                                    file, project.full_path, db_session
                                )
                                processed_files += 1
                            except Exception as e:
                                logger.error(f"Error processing file {file.path}: {e}")

                        try:
                            await db_session.commit()
                            logger.debug(f"Committed batch of {len(files_batch)} files")
                        except exc.SQLAlchemyError as e:
                            logger.error(f"Database commit error: {e}")
                            await db_session.rollback()
                            raise

            total_time = time.time() - start_time
            logger.info(
                f"Successfully processed {processed_files} files in {total_time:.2f} seconds"
            )

        except Exception as e:
            logger.error(f"Critical error during data processing: {e}")
            await db_session.rollback()
            raise

    async def _process_single_file(
        self, file, project_path: str, db_session: AsyncSession
    ):
        """Обработать один файл - получить коммит и сохранить данные"""
        try:
            commit_data = await self.get_commit_for_file(project_path, file.path)

            if commit_data:
                return await self._save_data(
                    commit_data.model_dump(), file.id, db_session
                )
            else:
                logger.error(f"No commit data found for file: {file.path}")
                raise

        except Exception as e:
            logger.error(f"error processing file {file.path} (ID: {file.id}): {e}")
            raise

    async def _save_data(
        self, data: dict[str, Any], file_id: int, db_session: AsyncSession
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
            author_id = author_entry.id

            created_commit = await commit_repo.create_commit(
                data=data, author_id=author_id
            )
            if not created_commit:
                logger.error(f"Failed to create or find commit for file ID {file_id}")
                return False
            commit_id = created_commit.id

            if await file_repo.update_file(file_id=file_id, commit_id=commit_id):
                logger.info(
                    f"Successfully linked commit {commit_id} to file {file_id}"
                )
                return True
            return False

        except exc.SQLAlchemyError as e:
            logger.error(
                f"Database error saving commit data for file ID {file_id}: {e}"
            )
            await db_session.rollback()
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error saving commit data for file ID {file_id}: {e}"
            )
            await db_session.rollback()
            raise
