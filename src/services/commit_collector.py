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

    """Класс для сбора и обработки информации о коммитах файлов через GitLab API."""

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
        """Кодирует путь проекта для безопасного использования в URL-запросах."""
        return quote(project_path, safe="")

    async def _wait_for_retry(
        self, attempt: int, response: Optional[aiohttp.ClientResponse] = None
    ):
        """Вычисляет время ожидания и засыпает."""
        retry_after = self.config.retry_delay * (2**attempt)

        if response and response.status == 429:
            ra = response.headers.get("Retry-After")
            try:
                retry_after = int(ra) if ra is not None else retry_after
            except ValueError:
                pass
            logger.warning(f"Rate limited by GitLab API. Retry after {retry_after}s")
        elif response:
            logger.warning(
                f"GitLab temporary error {response.status} on attempt "
                f"{attempt + 1}/{self.config.max_retries}"
            )

        await asyncio.sleep(retry_after)

    async def _make_gitlab_request(self, url: str, params: dict) -> Optional[Any]:
        """Выполняет запрос к API с обработкой ошибок, таймаутов и ограничений частоты запросов."""
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)

        for attempt in range(self.config.max_retries):
            try:
                async with aiohttp.ClientSession(
                    headers=self.headers, timeout=timeout
                ) as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            return await response.json()

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

                        if response.status == 429 or response.status in (
                            500,
                            502,
                            503,
                            504,
                        ):
                            if attempt < self.config.max_retries - 1:
                                await self._wait_for_retry(attempt, response)
                                continue
                            return None

                        body = await response.text()
                        logger.error(f"Error {response.status} from GitLab API: {body}")
                        return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Network/Timeout error on attempt {attempt + 1}/{self.config.max_retries}: {e}"
                )
                if attempt < self.config.max_retries - 1:
                    await self._wait_for_retry(attempt)
                    continue
                logger.error(f"Max retries exceeded: {e}")
                return None

        return None

    async def get_commit_for_file(
        self, project: Project, file: File
    ) -> Optional[GitLabCommitData]:
        """Запрашивает у GitLab API информацию о последнем коммите для указанного файла."""
        try:
            project_path = project.full_path
            file_path = file.path
            encoded_project_path = self._encode_project_path(project_path)
            url = f"{self.config.rest_url}/projects/{encoded_project_path}/repository/commits"

            params = {
                "path": file_path,
                "page": 1,
                "per_page": 1,
            }

            commits = await self._make_gitlab_request(url, params)
            if not commits:
                logger.warning(
                    f"No commits found for file {file_path} (ID: {file.id}) "
                    f"in project {project_path} (ID: {project.id})"
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

    async def _process_project_files(
        self, project: Project, db_session: AsyncSession
    ) -> tuple[int, int, int]:
        """Обрабатывает файлы одного проекта и возвращает статистику."""
        file_repo = FileRepository(db_session)
        p_ok, p_failed, p_skipped = 0, 0, 0

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
                            p_ok += 1
                        elif result is False:
                            p_failed += 1
                        else:  # None
                            p_skipped += 1

                except PermissionError:
                    raise
                except Exception as e:
                    p_failed += 1
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

        return p_ok, p_failed, p_skipped

    async def process_data(self, db_session: AsyncSession, run_id: int):
        """Запускает пакетную обработку файлов всех проектов текущего запуска для сбора коммитов."""
        project_repo = ProjectRepository(db_session)

        total_ok = 0
        total_failed = 0
        total_skipped = 0
        start_time = time.time()

        try:
            async for projects_batch in project_repo.iterate_projects_by_run(
                batch_size=4, run_id=run_id
            ):
                for project in projects_batch:
                    logger.info(
                        f"Commit processing project: {project.full_path} (ID: {project.id})"
                    )

                    p_ok, p_failed, p_skipped = await self._process_project_files(
                        project, db_session
                    )
                    total_ok += p_ok
                    total_failed += p_failed
                    total_skipped += p_skipped

            total_time = time.time() - start_time
            logger.info(
                f"Commit collector finished: ok={total_ok}, failed={total_failed}, "
                f"skipped={total_skipped}, time={total_time:.2f}s"
            )

        except Exception as e:
            logger.error(f"Critical error during commit processing: {e}")
            await db_session.rollback()
            raise

    async def _process_single_file(
        self, file: File, project: Project, db_session: AsyncSession
    ) -> bool:
        """Получает данные коммита для конкретного файла и инициирует их сохранение."""
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
        """Создает записи автора и коммита в БД, после чего связывает коммит с файлом."""
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
