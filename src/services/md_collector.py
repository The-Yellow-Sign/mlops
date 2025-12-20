import os
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import File
from db.repository import FileRepository, ProjectRepository

load_dotenv()


class GitLabMDCollector:
    def __init__(self):
        self.base_url = "https://gitlab.com/api/v4/projects"
        self.token = os.getenv("GITLAB_TOKEN")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _encode_project_path(self, project_path: str) -> str:
        return quote(project_path, safe="")

    async def get_raw_file(
        self, project_path: str, file_path: str
    ) -> Optional[dict[str, Any]]:
        """Получить содержимое MD файла."""
        encoded_project_path = self._encode_project_path(project_path)
        encoded_file_path = self._encode_project_path(file_path)

        url = f"{self.base_url}/{encoded_project_path}/repository/files/{encoded_file_path}/raw"

        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        if not content:
                            print(
                                f"There is no content in the markdown file {file_path} of the project {project_path}"
                            )
                            return None
                    else:
                        print(f"Error {response.status} for file {file_path}")
                        error_text = await response.text()
                        print(f"Error details: {error_text}")
                        return None
            return content
        except aiohttp.ClientError as e:
            print(
                f"Client error fetching raw file {file_path} of the project {project_path}: {e}"
            )
            return None
        except Exception as e:
            print(
                f"Unexpected error fetching raw file{file_path} of the project {project_path}: {e}"
            )
            return None

    async def process_data(self, db_session: AsyncSession):
        """Обработать данные."""
        file_repo = FileRepository(db_session)
        project_repo = ProjectRepository(db_session)

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
                            raw_file = await self.get_raw_file(project_path, file_path)

                            if raw_file:
                                await self._save_data(
                                    raw_file, file, project_path, db_session
                                )

                            else:
                                print(f"No commit data found for file: {file_path}")
            await db_session.commit()
        except Exception as e:
            print(f"Unexpected error processing data: {e}")
            return None

    async def _save_data(
        self, data: str, file: File, project_path: str, db_session: AsyncSession
    ):
        """Сохранить данные в БД."""
        if not data:
            print("No raw file to save")
            return None

        file_repo = FileRepository(db_session)

        try:
            if await file_repo.update_file(file_id=file.id, raw_file=data):
                print(
                    f"Successfully added raw file {file.path} of the project {project_path}"
                )
        except Exception as e:
            print(f"Error saving raw file {file.path} with file ID {file.id}: {e}")
            return None


# TODO:
# 1. Сделать рефакторинг кода
# 2. Реализовать логику обновления данных (если существует, то проверить на индентичность).
