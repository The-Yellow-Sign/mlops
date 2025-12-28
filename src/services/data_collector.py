import asyncio
import logging
from typing import Any, Optional

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from sqlalchemy import exc
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository import FileRepository, GroupRepository, ProjectRepository
from db.schemas import GitLabConfig

load_dotenv()

logger = logging.getLogger(__name__)


class GitLabDataCollector:
    """Сборщик метаданных из GitLab GraphQL API."""

    def __init__(self, full_path: str, config: GitLabConfig):
        if not config.gitlab_token:
            logger.error("GITLAB_TOKEN is required")
            raise ValueError("GITLAB_TOKEN is required")

        self.config = config
        self.full_path = full_path
        self.headers = {
            "Authorization": f"Bearer {config.gitlab_token}",
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

    async def _make_graphql_request(
        self, variable_values: dict
    ) -> Optional[dict[str, Any]]:
        """Универсальный метод для запросов к GitLab GraphQL API с retry-логикой и обработкой ошибок"""
        transport = AIOHTTPTransport(
            url=self.config.graphql_url,
            headers=self.headers,
        )

        for attempt in range(self.config.max_retries):
            try:
                async with Client(
                    transport=transport,
                    fetch_schema_from_transport=False,
                    execute_timeout=self.config.request_timeout,
                ) as session:
                    return await session.execute(
                        gql(self.query), variable_values=variable_values
                    )

            except asyncio.TimeoutError as e:
                logger.warning(
                    f"GraphQL timeout on attempt {attempt + 1}/{self.config.max_retries}: {e}"
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (2**attempt))
                    continue
                return None

            except Exception as e:
                logger.warning(
                    f"GraphQL request error on attempt {attempt + 1}/{self.config.max_retries}: {e}"
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (2**attempt))
                    continue
                logger.error(f"Max retries exceeded for GraphQL request: {e}")
                return None

        return None

    async def collect_data(self) -> Optional[dict]:
        """Собрать метаданные данные из GitLab GraphQL API."""
        logger.info(f"Starting data collection for path: {self.full_path}")

        try:
            response = await self._make_graphql_request(
                {"fullPath": self.full_path, "ref": self.config.default_branch}
            )

            if response:
                logger.info(f"Successfully received data for path: {self.full_path}")
            else:
                logger.error(f"Failed to receive data for path: {self.full_path}")
            return response

        except Exception as e:
            logger.error(f"Unexpected error when receiving data: {e}")
            return None

    async def process_and_save_data(
        self, data: dict[str, Any], db_session: AsyncSession, run_id: int
    ) -> bool:
        if not data:
            logger.warning("No data to process and save")
            return False

        group_repo = GroupRepository(db_session)
        project_repo = ProjectRepository(db_session)
        file_repo = FileRepository(db_session)

        saved_projects = 0
        failed_projects = 0

        try:
            group_data = data.get("group")
            project_data = data.get("project")

            if group_data:
                await group_repo.process_group(group_data)

                projects_data = group_data.get("projects", {}).get("nodes", [])
                logger.info(f"Found {len(projects_data)} projects in groups")

                for prj in projects_data:
                    try:
                        async with db_session.begin_nested():
                            project_group_gitlab_id = prj.get("group", {}).get("id")
                            group_id = None

                            if project_group_gitlab_id:
                                project_group = await group_repo.get_group_by_gitlab_id(
                                    project_group_gitlab_id
                                )
                                group_id = project_group.id if project_group else None

                            await project_repo.create_or_update_project(
                                prj, group_id, run_id
                            )
                            await self._collect_files(prj, file_repo, project_repo)
                            saved_projects += 1
                    except Exception as e:
                        failed_projects += 1
                        logger.error(
                            f"Failed to process project {prj.get('fullPath', prj.get('name'))}: {e}"
                        )

                await db_session.commit()
                logger.info(
                    f"Saved group '{group_data.get('name')}', projects ok={saved_projects}, failed={failed_projects}"
                )
                return True

            if project_data:
                try:
                    async with db_session.begin_nested():
                        await project_repo.create_or_update_project(
                            project_data, group_id=None, run_id=run_id
                        )
                        await self._collect_files(project_data, file_repo, project_repo)
                    await db_session.commit()
                    logger.info(
                        f"Successfully saved project '{project_data.get('name')}'"
                    )
                    return True
                except Exception as e:
                    logger.error(f"Failed to process single project: {e}")
                    await db_session.commit()
                    return False

            logger.warning("No group or project data found in response")
            return False

        except exc.SQLAlchemyError as e:
            logger.error(f"Database error during data processing: {e}")
            await db_session.rollback()
            raise

    async def _collect_files(
        self, data: dict[str, Any], file_repo, project_repo
    ) -> Optional[dict]:
        """Собрать данные файлов из GitLab GraphQL API"""
        files_data = (
            data.get("repository", {}).get("tree", {}).get("blobs", {}).get("nodes", [])
        )
        project_gitlab_id = data.get("id")

        if not project_gitlab_id:
            logger.warning(
                f"No project ID found for file collection in project: {data.get('name', 'unknown')}"
            )
            return

        project = await project_repo.get_project_by_gitlab_id(project_gitlab_id)
        if not project:
            logger.error(f"Project with gitlab_id {project_gitlab_id} not found in DB")
            return

        md_files = [f for f in files_data if f.get("name", "").lower().endswith(".md")]
        logger.info(f"Found {len(md_files)} .md files in project {project.full_path}")

        for file_data in md_files:
            try:
                await file_repo.create_file(data=file_data, project_id=project.id)
            except exc.IntegrityError as e:
                logger.warning(
                    f"Integrity error saving file {file_data.get('path', 'unknown')} "
                    f"in project {project.full_path}: {e}"
                )
                continue
            except exc.SQLAlchemyError as e:
                logger.error(
                    f"Database error saving file {file_data.get('path', 'unknown')} "
                    f"in project {project.full_path}: {e}"
                )
                raise
            except Exception as e:
                logger.error(
                    f"Error saving file {file_data.get('path', 'unknown')} in project {project.full_path}: {e}"
                )
                continue
