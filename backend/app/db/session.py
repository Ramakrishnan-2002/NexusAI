import os
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from app.core.config import settings
from app.core.logging import logger
from app.models.base import Base

# Configure database engine
db_url = settings.DATABASE_URL

# For testing / local development fallback:
if "sqlite" in db_url:
    engine = create_async_engine(
        db_url,
        echo=False,
        future=True,
    )
else:
    engine = create_async_engine(
        db_url,
        echo=False,
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        pool_pre_ping=True,
        future=True,
    )

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for yielding async database sessions with auto-rollback on error"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db_models():
    """Utility to initialize DB tables (used for tests and dev setup)"""
    try:
        async with engine.begin() as conn:
            # Enable pgvector extension if on postgresql
            if "postgresql" in str(engine.url):
                try:
                    from sqlalchemy import text
                    await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
                except Exception as ext_err:
                    logger.warning(f"Could not execute CREATE EXTENSION vector: {ext_err}")
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Database models initialized successfully.")
    except Exception as e:
        logger.warning(f"Database initialization deferred (broker/db may start later in compose): {e}")
