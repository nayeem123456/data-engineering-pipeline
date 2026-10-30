"""
Database session and connection engine manager.
Provides robust connection pooling for PostgreSQL with fallback to SQLite for local zero-dependency testing.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.common.config import config
from src.common.logger import get_logger

logger = get_logger("platform.db")

Base = declarative_base()


class DatabaseManager:
    """Manages database engines and sessions for metadata and serving layers."""

    _engine: Engine | None = None
    _session_factory: sessionmaker | None = None
    _is_sqlite: bool = False

    @classmethod
    def get_engine(cls) -> Engine:
        """Initialize or return the cached SQLAlchemy Engine."""
        if cls._engine is not None:
            return cls._engine

        # Try connecting to PostgreSQL first
        pg_url = config.DATABASE_URL
        try:
            test_engine = create_engine(
                pg_url, pool_pre_ping=True, connect_args={"connect_timeout": 3}
            )
            with test_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            cls._engine = test_engine
            cls._is_sqlite = False
            logger.info(
                "Successfully connected to PostgreSQL metadata store at %s", config.POSTGRES_HOST
            )
        except Exception as exc:
            if config.USE_SQLITE_METADATA_FALLBACK:
                logger.warning(
                    "PostgreSQL connection unavailable (%s). Falling back to SQLite metadata database at %s",
                    exc,
                    config.SQLITE_DB_PATH,
                )
                config.SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
                sqlite_url = f"sqlite:///{config.SQLITE_DB_PATH}"
                cls._engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})
                cls._is_sqlite = True
            else:
                logger.error("Failed to connect to PostgreSQL and fallback is disabled.")
                raise exc

        cls._session_factory = sessionmaker(bind=cls._engine, autoflush=False, autocommit=False)
        return cls._engine

    @classmethod
    def is_sqlite(cls) -> bool:
        """Check if currently running on SQLite fallback."""
        if cls._engine is None:
            cls.get_engine()
        return cls._is_sqlite

    @classmethod
    def reset_engine(cls) -> None:
        """Reset engine (useful for testing)."""
        cls._engine = None
        cls._session_factory = None
        cls._is_sqlite = False

    @classmethod
    @contextmanager
    def get_session(cls) -> Generator[Session, None, None]:
        """Context manager providing a transactional database session."""
        if cls._session_factory is None:
            cls.get_engine()

        assert cls._session_factory is not None
        session: Session = cls._session_factory()
        try:
            yield session
            session.commit()
        except Exception as exc:
            session.rollback()
            logger.error("Transaction rolled back due to error: %s", exc)
            raise
        finally:
            session.close()


db_manager = DatabaseManager
