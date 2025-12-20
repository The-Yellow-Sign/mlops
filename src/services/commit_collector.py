import os
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository import (
    AuthorRepository,
    CommitRepository,
    FileRepository,
    ProjectRepository,
)

load_dotenv()


class GitLabCommitCollector:
    def __init__(self):
        self.base_url = "https://gitlab.com/api/v4/projects"
        self.token = os.getenv("GITLAB_TOKEN")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _encode_project_path(self, project_path: str) -> str:
        return quote(project_path, safe="")

    async def get_commit_for_file(
        self, project_path: str, file_path: str
    ) -> Optional[dict[str, Any]]:
        encoded_project_path = self._encode_project_path(project_path)
        url = f"{self.base_url}/{encoded_project_path}/repository/commits"

        params = {
            "path": file_path,
            "page": 1,
            "per_page": 1,
            "ref_name": "main",
        }

        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        commits = await response.json()
                        if not commits:
                            print(
                                f"No commits found for file {file_path} in project {project_path}"
                            )
                            return None
                    else:
                        print(f"Error {response.status} for file {file_path}")
                        error_text = await response.text()
                        print(f"Error details: {error_text}")
                        return None

            return commits[0]
        except aiohttp.ClientError as e:
            print(
                f"Client error fetching commit for {file_path} in project {project_path}: {e}"
            )
            return None
        except Exception as e:
            print(
                f"Unexpected error fetching commit for {file_path} in project {project_path}: {e}"
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
                            file_id = file.id
                            file_path = file.path
                            commit_data = await self.get_commit_for_file(
                                project_path, file_path
                            )

                            if commit_data:
                                await self._save_data(commit_data, file_id, db_session)
                            else:
                                print(f"No commit data found for file: {file_path}")
            await db_session.commit()
        except Exception as e:
            print(f"Unexpected error processing data: {e}")
            return None

    async def _save_data(
        self, data: dict[str, Any], file_id: int, db_session: AsyncSession
    ):
        """Сохранить данные в БД."""
        if not data:
            print("No commit data to save")
            return None

        commit_repo = CommitRepository(db_session)
        author_repo = AuthorRepository(db_session)
        file_repo = FileRepository(db_session)

        try:
            await author_repo.create_author(data)
            author_entry = await author_repo.get_author_by_username(data["author_name"])
            if not author_entry:
                print(f"Failed to retrieve or create author: {data['author_name']}")
                return None
            author_id = author_entry.id

            created_commit = await commit_repo.create_commit(
                data=data, author_id=author_id
            )
            if not created_commit:
                print("Failed to create or find commit.")
                return None
            commit_id = created_commit.id

            if await file_repo.update_file(file_id=file_id, commit_id=commit_id):
                print(f"Successfully linked commit {commit_id} to file {file_id}")

        except Exception as e:
            print(f"Error saving commit data for file ID {file_id}: {e}")
            # В зависимости от логики, возможно, стоит откатить транзакцию db_session.rollback()
            return None
