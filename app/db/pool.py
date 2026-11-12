from contextlib import asynccontextmanager
from typing import AsyncGenerator
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from core.config import settings
from core.logging import logger
from interfaces.queries import INIT_SCHEMA_SQL, HEALTH_CHECK_SQL

# Global asynchronous connection pool instance
_pool: AsyncConnectionPool | None = None

async def init_db_pool() -> None:
    """Initializes the PostgreSQL connection pool."""
    global _pool
    if _pool is not None:
        return

    logger.info("Initializing PostgreSQL connection pool...")
    try:
        _pool = AsyncConnectionPool(
            conninfo=settings.database_url,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
            open=False,
            kwargs={
                "row_factory": dict_row,
                "autocommit": False
            }
        )
        await _pool.open()
        logger.info("PostgreSQL connection pool initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL pool: {e}. Running with database disconnected.")
        _pool = None

async def close_db_pool() -> None:
    """Gracefully closes all connections in the pool on shutdown."""
    global _pool
    if _pool is not None:
        logger.info("Closing PostgreSQL connection pool...")
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL connection pool closed.")

@asynccontextmanager
async def get_db_connection() -> AsyncGenerator[psycopg.AsyncConnection, None]:
    """Async context manager that yields a pooled PostgreSQL connection."""
    global _pool
    if _pool is None:
        raise RuntimeError("Database connection pool is not initialized or database is unreachable.")

    async with _pool.connection() as conn:
        try:
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

async def check_db_health() -> bool:
    """Pings the database with a lightweight query to verify connectivity."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(HEALTH_CHECK_SQL)
                return True
    except Exception as e:
        logger.warning(f"Database health check failed: {e}")
        return False

async def init_db_schema() -> None:
    """Idempotently initializes the PostgreSQL schema for tracking shipment compliance runs."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(INIT_SCHEMA_SQL)
        logger.info("PostgreSQL schema verified and ready.")
    except Exception as e:
        logger.error(f"Error running database schema initialization: {e}")
