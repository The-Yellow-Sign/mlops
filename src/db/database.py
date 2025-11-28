from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import Base

DATABASE_URL = "postgresql+asyncpg://postgres:password@localhost:5432/db"

engine = create_async_engine(DATABASE_URL, echo=False)

async_session = async_sessionmaker(engine, expire_on_commit=False)


async def get_db_session():
    """Получить асинхронную сессию БД."""
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()


async def create_tables():
    """Создать все таблицы в БД."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_tables():
    """Удалить все таблицы."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
