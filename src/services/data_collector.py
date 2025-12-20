import os
from typing import Any, Optional

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository import FileRepository, GroupRepository, ProjectRepository

load_dotenv()


class GitLabDataCollector:
    def __init__(self, full_path: str):
        self.url = "https://gitlab.com/api/graphql/"
        self.token = os.getenv("GITLAB_TOKEN")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        self.query = """
            query GetGroupOrProject($fullPath: ID!) {

                # ===== TRY AS GROUP =====
                group(fullPath: $fullPath) {
                    id
                    name
                    fullPath
                    webUrl

                    descendantGroups {
                        nodes {
                            id
                            name
                            fullPath
                            webUrl
                            parent {
                                id
                            }
                        }
                    }

                    projects(includeSubgroups: true) {
                        nodes {
                            id
                            name
                            fullPath
                            webUrl
                            group {
                                id
                            }

                            repository {
                                tree(ref: "main", recursive: true) {
                                    blobs {
                                        nodes {
                                            id
                                            name
                                            path
                                            sha
                                            type
                                            webUrl
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                # ===== TRY AS PROJECT =====
                project(fullPath: $fullPath) {
                    id
                    name
                    fullPath
                    webUrl

                    repository {
                        tree(ref: "main", recursive: true) {
                            blobs {
                                nodes {
                                    id
                                    name
                                    path
                                    sha
                                    type
                                    webUrl
                                }
                            }
                        }
                    }
                }
            }
        """
        self.full_path = full_path

    async def collect_data(self) -> Optional[dict]:
        """Собрать метаданные данные из GitLab GraphQL API."""
        # Select your transport with a defined url endpoint
        transport = AIOHTTPTransport(
            url=self.url,
            headers=self.headers,
        )

        # Create a GraphQL client using the defined transport
        async with Client(
            transport=transport, fetch_schema_from_transport=False
        ) as session:
            try:
                response = await session.execute(
                    gql(self.query), variable_values={"fullPath": self.full_path}
                )
                return response
            except Exception as e:
                print(f"Ошибка при получении данных:\n{e}")
                return None

    async def process_and_save_data(
        self, data: dict[str, Any], db_session: AsyncSession
    ) -> Optional[dict]:
        """Обработать и сохранить данные в БД."""
        if not data:
            return

        group_repo = GroupRepository(db_session)
        project_repo = ProjectRepository(db_session)
        file_repo = FileRepository(db_session)

        group_data = data.get("group")
        project_data = data.get("project")

        if group_data:
            await group_repo.process_group(group_data)
            projects_data = group_data.get("projects", {}).get("nodes", [])
            for project_data in projects_data:
                project_group_gitlab_id = project_data.get("group", {}).get("id")

                if project_group_gitlab_id:
                    project_group = await group_repo.get_group_by_gitlab_id(
                        project_group_gitlab_id
                    )
                    if project_group:
                        await project_repo.create_or_update_project(
                            project_data, project_group.id
                        )
                await self.collect_files(project_data, file_repo, project_repo)

            await db_session.commit()
            print(
                f"Успешно сохранено: группа '{group_data['name']}' с {len(projects_data)} проектами"
            )

        elif project_data:
            await project_repo.create_or_update_project(project_data)

    async def collect_files(
        self, data: dict[str, Any], file_repo, project_repo
    ) -> Optional[dict]:
        """Собрать данные файлов из GitLab GraphQL API"""
        files_data = (
            data.get("repository", {}).get("tree", {}).get("blobs", {}).get("nodes", [])
        )
        for file_data in files_data:
            if file_data.get("name")[-3:].lower() == ".md":
                file_project_gitlab_id = data.get("id")
                if file_project_gitlab_id:
                    file_project = await project_repo.get_project_by_gitlab_id(
                        file_project_gitlab_id
                    )
                    if file_project:
                        await file_repo.create_file(
                            data=file_data, project_id=file_project.id
                        )
