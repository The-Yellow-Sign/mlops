import asyncio
import logging
import os
import time

from dotenv import load_dotenv

from db.database import create_tables, drop_tables, get_db_session
from db.schemas import GitLabConfig
from services.commit_collector import GitLabCommitCollector
from services.data_collector import GitLabDataCollector
from services.md_collector import GitLabMDCollector

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app.log")],
)

logger = logging.getLogger(__name__)


async def main():
    start_time = time.time()
    full_path = "the-yellow-sign-test"
    # await drop_tables()
    await create_tables()

    config = GitLabConfig(gitlab_token=os.getenv("GITLAB_TOKEN"))
    collector_data = GitLabDataCollector(full_path=full_path, config=config)

    collector_commits = GitLabCommitCollector(config=config)
    collector_mds = GitLabMDCollector(config=config)

    data = await collector_data.collect_data()

    if data:
        async for db_session in get_db_session():
            await collector_data.process_and_save_data(data, db_session)
            await collector_commits.process_data(db_session)
            await collector_mds.process_data(db_session)
    else:
        print("Не удалось получить данные из GitLab")

    total_time = time.time() - start_time
    logger.info(
        f"Total data collection and uploading time took {total_time:.2f} seconds"
    )


if __name__ == "__main__":
    asyncio.run(main())
