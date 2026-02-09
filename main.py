import asyncio
import logging
import os
import time

from dotenv import load_dotenv

from db.database import create_tables, get_db_session
from db.repository import GroupRepository, RunRepository
from db.schemas import GitLabConfig
from services.commit_collector import GitLabCommitCollector
from services.data_collector import GitLabDataCollector
from services.discoverers import GitLabGraphQLDiscoverer
from services.md_collector import GitLabMDCollector

load_dotenv()

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/app.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


async def process_single_project(full_path, config, db_session):
    """Выполняет полный цикл сбора данных для конкретного проекта."""
    logger.info(f"Starting processing for project: {full_path}")

    run_repo = RunRepository(db_session)
    run = await run_repo.get_or_create_active_run(full_path=full_path)
    await db_session.commit()

    logger.info(f"Using active run_id={run.id} for full_path={full_path}")

    collector_data = GitLabDataCollector(full_path=full_path, config=config)
    collector_commits = GitLabCommitCollector(config=config)
    collector_mds = GitLabMDCollector(config=config)

    data = await collector_data.collect_data()
    if not data:
        logger.warning(f"Skipping {full_path}: failed to fetch data")
        return

    await collector_data.process_and_save_data(data, db_session, run_id=run.id)
    await collector_commits.process_data(db_session, run_id=run.id)
    await collector_mds.process_data(db_session, run_id=run.id)

    logger.info(f"Project {full_path} successfully processed")


async def main():
    """Основная точка входа."""
    start_time = time.time()
    await create_tables()

    config = GitLabConfig(
        rest_url=os.getenv("REST_URL"),
        graphql_url=os.getenv("GRAPHQL_URL"),
        gitlab_token=os.getenv("GITLAB_TOKEN"),
    )

    target_path = os.getenv("GITLAB_FULL_PATH", "the-yellow-sign-test")

    async for db_session in get_db_session():
        discoverer = GitLabGraphQLDiscoverer(config)
        if target_path.upper() == "ALL":
            logger.info("=== Phase 1: Creating Group Hierarchy ===")
            groups_meta = await discoverer.get_all_group_metadata()
            group_repo = GroupRepository(db_session)

            for meta in groups_meta:
                try:
                    # Используем существующий метод репозитория для сохранения
                    await group_repo.create_or_update_group(meta)
                except Exception as e:
                    logger.error(
                        f"Failed to register group {meta.get('fullPath')}: {e}"
                    )

            await db_session.commit()
            logger.info(f"Registered {len(groups_meta)} groups.")

            paths_to_process = await discoverer.get_all_project_paths()
        else:
            paths_to_process = [target_path]

        logger.info(f"=== Phase 2: Processing {len(paths_to_process)} projects ===")
        for path in paths_to_process:
            try:
                await process_single_project(path, config, db_session)
            except Exception as e:
                logger.error(f"Critical error processing {path}: {e}")
                continue

    total_time = time.time() - start_time
    logger.info(f"Total time: {total_time:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
