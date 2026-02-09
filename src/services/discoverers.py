import asyncio
import logging
from typing import Any, Optional

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from db.schemas import GitLabConfig

logger = logging.getLogger(__name__)


class GitLabGraphQLDiscoverer:

    """Сервис обнаружения через GitLab GraphQL API."""

    def __init__(self, config: GitLabConfig):
        if not config.gitlab_token:
            logger.error("GITLAB_TOKEN is required")
            raise ValueError("GITLAB_TOKEN is required")

        self.config = config
        self.url = self.config.graphql_url
        self.headers = {
            "Authorization": f"Bearer {config.gitlab_token}",
            "Content-Type": "application/json",
        }

    async def _make_graphql_request(
        self, query: str, variable_values: dict
    ) -> Optional[dict[str, Any]]:
        """Выполняет GraphQL-запрос с автоматическими повторами и обработкой ошибок."""
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
                        gql(query), variable_values=variable_values
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

    async def get_all_group_metadata(self) -> list[dict]:
        """Получает метаданные всех групп через GraphQL для создания структуры в БД."""
        groups_metadata = []
        has_next_page = True
        cursor = None

        query = """
        query($cursor: String) {
          groups(first: 100, after: $cursor) {
            pageInfo { hasNextPage endCursor }
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
        }
        """

        logger.info(f"Starting GraphQL group discovery on {self.url}")
        try:
            while has_next_page:
                result = await self._make_graphql_request(query, {"cursor": cursor})
                if not result:
                    break

                data = result.get("data", {}).get("groups") or result.get("groups")

                for node in data["nodes"]:
                    groups_metadata.append(node)

                has_next_page = data["pageInfo"]["hasNextPage"]
                cursor = data["pageInfo"]["endCursor"]

            return groups_metadata
        except Exception as e:
            logger.error(f"GraphQL group discovery error: {e}")
            return []

    async def get_all_project_paths(self) -> list[str]:
        """Получает пути всех проектов через GraphQL."""
        paths = []
        has_next_page = True
        cursor = None

        query = """
        query($cursor: String) {
          projects(membership: true, first: 100, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              fullPath
            }
          }
        }
        """

        logger.info(f"Starting GraphQL project discovery on {self.url}")
        try:
            while has_next_page:
                result = await self._make_graphql_request(query, {"cursor": cursor})
                if not result:
                    break

                data = result.get("data", {}).get("projects") or result.get("projects")

                for node in data["nodes"]:
                    paths.append(node["fullPath"])

                has_next_page = data["pageInfo"]["hasNextPage"]
                cursor = data["pageInfo"]["endCursor"]
                if has_next_page:
                    logger.info(f"Collected {len(paths)} project paths...")

            logger.info(f"GraphQL Discovery finished. Found projects: {len(paths)}")
            return paths
        except Exception as e:
            logger.error(f"GraphQL project discovery error: {e}")
            return []
