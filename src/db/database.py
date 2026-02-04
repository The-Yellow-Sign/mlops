import logging
import os

from dotenv import load_dotenv
from sqlalchemy import exc, inspect
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import Base

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/app.log"),
    ],
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

logger.info(f"Connecting to database at: {DATABASE_URL}")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def get_db_session():
    """Асинхронный генератор сессии БД с автоматическим откатом транзакции при ошибках."""
    async with async_session() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            await session.close()


def check_and_create(sync_conn):
    """Синхронно проверяет структуру БД и создает отсутствующие таблицы."""
    inspector = inspect(sync_conn)
    existing_tables = inspector.get_table_names()
    expected_tables = Base.metadata.tables.keys()

    if set(expected_tables).issubset(set(existing_tables)):
        return False

    Base.metadata.create_all(sync_conn)
    return True


async def create_tables():
    """Асинхронно инициализирует схему базы данных, создавая таблицы при необходимости."""
    try:
        async with engine.begin() as conn:
            tables_created = await conn.run_sync(check_and_create)

        if tables_created:
            logger.info("Tables created successfully (or schema updated)")
        else:
            logger.info("Tables already exist, skipping creation")

    except exc.SQLAlchemyError as e:
        logger.error(f"Error checking/creating tables: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during table creation: {e}")
        raise


async def drop_tables():
    """Асинхронно удаляет все таблицы, определенные в метаданных моделей."""
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
