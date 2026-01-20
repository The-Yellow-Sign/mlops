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
        else:
            # Для сетевых ошибок и таймаутов
            pass

        await asyncio.sleep(retry_after)

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

                        if response.status in (401, 403):
                            body = await response.text()
                            raise PermissionError(
                                f"GitLab auth error {response.status}: {body}"
                            )

                        if response.status == 404:
                            body = await response.text()
                            logger.warning(f"GitLab 404 for url={url}: {body}")
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

    async def get_raw_file(self, project_path: str, file_path: str) -> Optional[str]:
        """Загружает сырое текстовое содержимое файла по его пути."""
        encoded_project_path = self._encode_project_path(project_path)
        encoded_file_path = self._encode_project_path(file_path)

        url = (
            f"{self.config.rest_url}/{encoded_project_path}"
            f"/repository/files/{encoded_file_path}/raw"
        )

        try:
            content = await self._make_gitlab_request(url)
            if not content:
                logger.warning(
                    f"There is no content in the markdown file {file_path} "
                    f"of the project {project_path}"
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

    async def _process_single_file(
        self, file: File, project_path: str, db_session: AsyncSession
    ) -> Optional[bool]:
        """Обрабатывает один файл: получает контент и сохраняет его."""
        raw_file = await self.get_raw_file(project_path, file.path)
        if not raw_file:
            return None  # Failed to get content

        return await self._save_data(raw_file, file, project_path, db_session)

    async def _process_project(
        self, project, db_session: AsyncSession
    ) -> tuple[int, int]:
        """Обрабатывает файлы одного проекта и возвращает статистику (ok, failed)."""
        file_repo = FileRepository(db_session)
        project_ok = 0
        project_failed = 0

        async for files_batch in file_repo.iterate_files_by_project(
            project.id, batch_size=7
        ):
            for file in files_batch:
                try:
                    async with db_session.begin_nested():
                        result = await self._process_single_file(
                            file, project.full_path, db_session
                        )

                        if result is True:
                            project_ok += 1
                        else:
                            # False или None считаем за ошибку в контексте
                            # сбора контента
                            project_failed += 1

                except PermissionError:
                    raise
                except Exception as e:
                    project_failed += 1
                    logger.error(f"Error processing file {file.path}: {e}")

            try:
                await db_session.commit()
                logger.debug(f"Committed batch of {len(files_batch)} files")
            except exc.SQLAlchemyError as e:
                logger.error(f"Database commit error: {e}")
                await db_session.rollback()
                raise

        return project_ok, project_failed

    async def process_data(self, db_session: AsyncSession, run_id: int):
        """Итерирует по файлам проектов и обновляет их контент в базе данных."""
        project_repo = ProjectRepository(db_session)

        total_ok = 0
        total_failed = 0
        start_time = time.time()

        try:
            async for projects_batch in project_repo.iterate_projects_by_run(
                run_id=run_id, batch_size=4
            ):
                for project in projects_batch:
                    p_ok, p_failed = await self._process_project(project, db_session)
                    total_ok += p_ok
                    total_failed += p_failed

            total_time = time.time() - start_time
            logger.info(
                f"MD collector finished: ok={total_ok}, failed={total_failed}, "
                f"time={total_time:.2f}s"
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
