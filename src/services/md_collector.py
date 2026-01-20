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

from db.models import File
from db.repository import FileRepository, ProjectRepository
from db.schemas import GitLabConfig

load_dotenv()

logger = logging.getLogger(__name__)


class GitLabMDCollector:
    """Сборщик содержимого Markdown-файлов через GitLab REST API."""

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
        """Кодирует путь проекта для использования в URL REST API."""
        return quote(project_path, safe="")

    async def _make_gitlab_request(self, url: str) -> Optional[Any]:
        """Выполняет HTTP-запрос к GitLab API с retry-механизмом и обработкой ошибок."""
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)

        for attempt in range(self.config.max_retries):
            try:
                async with aiohttp.ClientSession(
                    headers=self.headers, timeout=timeout
                ) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            return await response.text()

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
                            logger.warning(f"GitLab 404 for url={url}: {body}")
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

    async def get_raw_file(self, project_path: str, file_path: str) -> Optional[str]:
        """Загружает сырое текстовое содержимое файла по его пути."""
        encoded_project_path = self._encode_project_path(project_path)
        encoded_file_path = self._encode_project_path(file_path)

        url = f"{self.config.rest_url}/{encoded_project_path}/repository/files/{encoded_file_path}/raw"

        try:
            content = await self._make_gitlab_request(url)
            if not content:
                logger.warning(
                    f"There is no content in the markdown file {file_path} of the project {project_path}"
                )
                return None

            if not isinstance(content, str):
                logger.warning(f"Unexpected response format for raw file: {content}")
                return None

            return content

        except PermissionError as e:
            logger.error(
                f"Permission error fetching raw file for {file_path} in project {project_path}: {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error fetching raw file for {file_path} in project {project_path}: {e}"
            )
            return None

    async def process_data(self, db_session: AsyncSession, run_id: int):
        """Итерирует по файлам проектов и обновляет их контент в базе данных."""
        file_repo = FileRepository(db_session)
        project_repo = ProjectRepository(db_session)

        processed_ok = 0
        processed_failed = 0
        processed_skipped = 0
        start_time = time.time()

        try:
            async for projects_batch in project_repo.iterate_projects_by_run(
                run_id=run_id, batch_size=4
            ):
                for project in projects_batch:
                    project_id = project.id
                    project_path = project.full_path

                    async for files_batch in file_repo.iterate_files_by_project(
                        project_id, batch_size=7
                    ):
                        for file in files_batch:
                            try:
                                async with db_session.begin_nested():
                                    raw_file = await self.get_raw_file(
                                        project_path, file.path
                                    )
                                    if not raw_file:
                                        processed_failed += 1
                                        continue

                                    result = await self._save_data(
                                        raw_file, file, project_path, db_session
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
                f"MD collector finished: ok={processed_ok}, failed={processed_failed}, "
                f"skipped={processed_skipped}, time={total_time:.2f}s"
            )

        except Exception as e:
            logger.error(f"Critical error during data processing: {e}")
            await db_session.rollback()
            raise

    async def _save_data(
        self, data: str, file: File, project_path: str, db_session: AsyncSession
    ) -> bool:
        """Сохраняет полученный контент файла в базу данных."""
        if not data:
            logger.warning("Attempt to save empty raw file")
            return False

        file_repo = FileRepository(db_session)

        try:
            updated = await file_repo.update_file(file_id=file.id, raw_file=data)
            if updated is True:
                logger.info(
                    f"Successfully added raw file {file.path} of the project {project_path}"
                )
                return True

            return None

        except (ValidationError, exc.SQLAlchemyError) as e:
            logger.error(
                f"Database/validation error saving raw file {file.path} with file ID {file.id}: {e}"
            )
            return False

        except Exception as e:
            logger.error(
                f"Unexpected error saving raw file {file.path} with file ID {file.id}: {e}"
            )
            return False
