import asyncio

from db.database import create_tables, drop_tables, get_db_session
from services.commit_collector import GitLabCommitCollector
from services.data_collector import GitLabDataCollector
from services.md_collector import GitLabMDCollector


async def main():
    full_path = "the-yellow-sign-test"
    await drop_tables()
    await create_tables()

    collector_data = GitLabDataCollector(full_path)
    collector_commits = GitLabCommitCollector()
    collector_mds = GitLabMDCollector()

    data = await collector_data.collect_data()

    if data:
        async for db_session in get_db_session():
            await collector_data.process_and_save_data(data, db_session)
            await collector_commits.process_data(db_session)
            await collector_mds.process_data(db_session)
    else:
        print("Не удалось получить данные из GitLab")


if __name__ == "__main__":
    asyncio.run(main())
