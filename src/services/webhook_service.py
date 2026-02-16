import asyncio
import logging
import os
from typing import Annotated, Any, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Request
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import async_session
from db.repository import GroupRepository
from db.schemas import GitLabConfig
from main import process_single_project

logger = logging.getLogger(__name__)
app = FastAPI()

WEBHOOK_SECRET = os.getenv("GITLAB_WEBHOOK_SECRET")


def get_gitlab_config() -> GitLabConfig:
    """Зависимость для получения конфигурации из окружения."""
    return GitLabConfig(
        rest_url=os.getenv("REST_URL"),
        graphql_url=os.getenv("GRAPHQL_URL"),
        gitlab_token=os.getenv("GITLAB_TOKEN"),
    )


PROJECT_EVENTS = {
    "project_create",
    "project_update",
    "project_rename",
    "project_transfer",
    "project_import",
}

GROUP_EVENTS = {
    "group_create",
    "group_update",
    "group_rename",
}

PUSH_EVENTS = {"push", "tag_push"}

ALL_EVENTS = PROJECT_EVENTS | GROUP_EVENTS | PUSH_EVENTS


class GitLabGraphQLClient:

    """Клиент для работы с GitLab GraphQL API."""

    def __init__(self, config: GitLabConfig):
        self.config = config
        self.headers = {"Authorization": f"Bearer {config.gitlab_token}"}

    async def _make_graphql_request(
        self, query: str, variable_values: dict
    ) -> Optional[dict[str, Any]]:
        """Выполняет GraphQL-запрос с автоматическими повторами и обработкой ошибок."""
        transport = AIOHTTPTransport(
            url=str(self.config.graphql_url),
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
            except Exception as e:
                logger.warning(
                    f"GraphQL request error on attempt {attempt + 1}/{self.config.max_retries}: {e}"
                )

            if attempt < self.config.max_retries - 1:
                await asyncio.sleep(self.config.retry_delay * (2**attempt))

        logger.error("Max retries exceeded for GraphQL request")
        return None

    async def get_group_details(self, full_path: str) -> Optional[dict]:
        """Получает полные данные группы, включая родителя, через GraphQL."""
        query = """
        query getGroupDetails($fullPath: ID!) {
          group(fullPath: $fullPath) {
            id
            name
            fullPath
            webUrl
            parent {
              id
            }
          }
        }
        """
        variables = {"fullPath": full_path}

        result = await self._make_graphql_request(query, variables)

        if result and "group" in result:
            return result["group"]
        return None


class WebhookProcessor:

    """Класс, инкапсулирующий логику обработки вебхуков."""

    def __init__(self, db_session: AsyncSession, config: GitLabConfig):
        self.db_session = db_session
        self.config = config
        self.graphql_client = GitLabGraphQLClient(config)
        self.group_repo = GroupRepository(db_session)

    async def process_push_event(self, payload: dict) -> dict:
        """Обрабатывает изменения в коде (push, tags)."""
        project_path = payload.get("project", {}).get("path_with_namespace")
        logger.info(
            f"Code change detected ({payload.get('event_name')}): {project_path}"
        )

        await process_single_project(project_path, self.config, self.db_session)
        return {"status": "ok", "detail": f"Project {project_path} synced"}

    async def process_project_sync_event(self, payload: dict) -> dict:
        """Обрабатывает системные события изменений проекта."""
        project_path = payload.get("path_with_namespace")
        event = payload.get("event_name")

        logger.info(f"Project lifecycle event ({event}): {project_path}")

        await process_single_project(project_path, self.config, self.db_session)
        return {"status": "ok", "detail": f"Project {project_path} updated"}

    async def process_group_sync_event(self, payload: dict) -> dict:
        """Обрабатывает системные события изменений группы."""
        full_path = payload.get("full_path")
        event = payload.get("event_name")
        logger.info(f"Group lifecycle event ({event}): {full_path}")

        try:
            gql_data = await self.graphql_client.get_group_details(full_path)

            if gql_data:
                logger.info(f"Fetched details for group {full_path}")
                meta = {
                    "id": gql_data["id"],
                    "name": gql_data["name"],
                    "fullPath": gql_data["fullPath"],
                    "webUrl": gql_data["webUrl"],
                    "parent": gql_data.get("parent"),
                }
            else:
                logger.warning(
                    f"GraphQL failed for {full_path}, using payload fallback"
                )

                base_url = str(self.config.rest_url).rstrip("/")
                if "/api/v4" in base_url:
                    base_url = base_url.split("/api/v4")[0]

                group_id = payload.get("group_id")

                meta = {
                    "id": f"gid://gitlab/Group/{group_id}",
                    "name": payload.get("name"),
                    "fullPath": full_path,
                    "webUrl": f"{base_url}/{full_path}",
                    "parent": None,
                }

            logger.info(f"=== Webhook: Registering Group {meta['fullPath']} ===")
            await self.group_repo.create_group(meta)
            await self.db_session.commit()

            return {"status": "ok", "detail": "Group synced"}

        except Exception as e:
            logger.error(f"Failed to process group sync: {e}")
            await self.db_session.rollback()
            raise HTTPException(status_code=500, detail=str(e)) from e


async def background_worker(event_type: str, payload: dict, config: GitLabConfig):
    """Единый воркер распределяет задачи по типам событий."""
    logger.info(f"Background task started for {event_type}")

    async with async_session() as session:
        try:
            processor = WebhookProcessor(session, config)

            if event_type in PUSH_EVENTS:
                await processor.process_push_event(payload)

            elif event_type in PROJECT_EVENTS:
                await processor.process_project_sync_event(payload)

            elif event_type in GROUP_EVENTS:
                await processor.process_group_sync_event(payload)

            logger.info(f"Background task finished for {event_type}")

        except Exception as e:
            logger.error(f"Background task failed: {e}")


@app.post("/webhook")
async def git_event_handler(
    request: Request,
    background_tasks: BackgroundTasks,
    config: Annotated[GitLabConfig, Depends(get_gitlab_config)],
    x_gitlab_token: Annotated[str | None, Header()] = None,
):
    """Асинхронный маршрутизатор вебхуков."""
    if x_gitlab_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid token")

    payload = await request.json()
    event_type = payload.get("event_name") or payload.get("object_kind")

    logger.info(f"Received webhook: {event_type}. Queuing background task.")

    if event_type in ALL_EVENTS:
        logger.info(f"Event '{event_type}' is allowed. Queuing background task.")
        background_tasks.add_task(background_worker, event_type, payload, config)
        return {"status": "accepted", "detail": "Processing started in background"}

    logger.info(f"Event '{event_type}' is ignored.")
    return {"status": "ignored", "event": event_type}
