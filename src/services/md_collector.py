import asyncio
import logging
import time
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import exc
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import File
from db.repository import FileRepository, ProjectRepository
from db.schemas import GitLabConfig

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app.log")],
)

logger = logging.getLogger(__name__)


class GitLabMDCollector:
    """Сборщик содержимого MD файлов из GitLab API."""

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

    async def _make_gitlab_request(self, url: str) -> Optional[dict[str, Any]]:
        for attempt in range(self.config.max_retries):
            try:
                async with aiohttp.ClientSession(headers=self.headers) as session:
                    async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(
                            total=self.config.request_timeout
                        ),
                    ) as response:
                        if response.status == 200:
                            return await response.text()
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

    async def get_raw_file(
        self, project_path: str, file_path: str
    ) -> Optional[dict[str, Any]]:
        """Получить содержимое MD файла."""
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

        except Exception as e:
            logger.error(
                f"Unexpected error fetching raw file for {file_path} in project {project_path}: {e}"
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
                    project_id = project.id
                    project_path = project.full_path

                    async for files_batch in file_repo.iterate_files_by_project(
                        project_id, batch_size=7
                    ):
                        for file in files_batch:
                            file_path = file.path
                            try:
                                raw_file = await self.get_raw_file(
                                    project_path, file_path
                                )

                                if raw_file:
                                    await self._save_data(
                                        raw_file, file, project_path, db_session
                                    )

                                else:
                                    logger.warning(
                                        f"No content found for file: {file_path}"
                                    )
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

    async def _save_data(
        self, data: str, file: File, project_path: str, db_session: AsyncSession
    ) -> bool:
        """Сохранить данные о коммите в БД с обработкой ошибок."""
        if not data:
            logger.warning("Attempt to save empty raw file")
            return False

        file_repo = FileRepository(db_session)

        try:
            if await file_repo.update_file(file_id=file.id, raw_file=data):
                logger.info(
                    f"Successfully added raw file {file.path} of the project {project_path}"
                )
                return True
        except Exception as e:
            logger.error(
                f"Error saving raw file {file.path} with file ID {file.id}: {e}"
            )
            return False


# TODO:
# 1. Сделать рефакторинг кода
# 2. Реализовать логику обновления данных (если существует, то проверить на индентичность)
# 3. Парсинг с репы, не только из группы.
