"""Database configuration helpers for the local ChemDB cache."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Runtime settings sourced from environment variables."""

    db_url: str = os.getenv("DB_URL", "postgresql+psycopg2://localhost/chemdb")
    pool_size: int = int(os.getenv("DB_POOL_SIZE", "10"))
    max_overflow: int = int(os.getenv("DB_MAX_OVERFLOW", "20"))
    echo_sql: bool = os.getenv("DB_ECHO_SQL", "false").lower() == "true"
    comptox_api_key: str | None = os.getenv("COMPTOX_API_KEY")


settings = Settings()


def get_engine() -> Engine:
    """Create a pooled SQLAlchemy engine backed by psycopg."""
    return create_engine(
        settings.db_url,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_pre_ping=True,
        echo=settings.echo_sql,
        future=True,
    )


@lru_cache(maxsize=1)
def _session_factory() -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(), autoflush=False, expire_on_commit=False, future=True)


def SessionLocal() -> Session:
    """Return a new ORM session from the pooled factory."""
    return _session_factory()()
