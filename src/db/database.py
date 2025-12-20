import logging
import os

from dotenv import load_dotenv
from sqlalchemy import exc
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import Base

load_dotenv()
logger = logging.getLogger(__name__)


DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/db"
)

engine = create_async_engine(DATABASE_URL, echo=False)

async_session = async_sessionmaker(engine, expire_on_commit=False)


async def get_db_session():
    """Получить асинхронную сессию БД."""
    async with async_session() as session:
        try:
            yield session
        except Exception as e:
            logger.error(f"Database session error: {e}")
        finally:
            await session.close()


async def create_tables():
    """Создать все таблицы в БД."""
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Tables created successfully")
    except exc.SQLAlchemyError as e:
        logger.error(f"Error creating tables: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during table creation: {e}")
        raise


async def drop_tables():
    """Удалить все таблицы."""
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        logger.info("Tables dropped successfully")
    except exc.SQLAlchemyError as e:
        logger.error(f"Error dropping tables: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during table dropping: {e}")
        raise
