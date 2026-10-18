"""SQLAlchemy models for unified CompTox + ECHA hazard data."""

from __future__ import annotations

import enum
import uuid
from datetime import date, datetime

from sqlalchemy import ARRAY, Date, DateTime, Enum, ForeignKey, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base class."""


class DataSource(str, enum.Enum):
    COMPTOX = "comptox"
    ECHA = "echa"
    MANUAL = "manual"


class Reliability(str, enum.Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    QSAR = "qsar"


class Substance(Base):
    __tablename__ = "substances"

    substance_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dtxsid: Mapped[str | None] = mapped_column(String, unique=True, nullable=True)
    cas_rn: Mapped[str | None] = mapped_column(String, nullable=True)
    ec_number: Mapped[str | None] = mapped_column(String, nullable=True)
    inchikey14: Mapped[str | None] = mapped_column(String, nullable=True)
    preferred_name: Mapped[str] = mapped_column(String, nullable=False)
    molecular_formula: Mapped[str | None] = mapped_column(String, nullable=True)
    is_uvcb: Mapped[bool] = mapped_column(default=False, nullable=False)
    data_sources: Mapped[list[DataSource]] = mapped_column(ARRAY(Enum(DataSource, name="data_source")), default=list)
    last_updated: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    synonyms: Mapped[list["SubstanceSynonym"]] = relationship(back_populates="substance", cascade="all, delete-orphan")
    hazard_endpoints: Mapped[list["HazardEndpoint"]] = relationship(back_populates="substance", cascade="all, delete-orphan")


class SubstanceSynonym(Base):
    __tablename__ = "substance_synonyms"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    substance_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("substances.substance_id", ondelete="CASCADE"), nullable=False)
    synonym: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[DataSource] = mapped_column(Enum(DataSource, name="data_source"), nullable=False)

    substance: Mapped[Substance] = relationship(back_populates="synonyms")


class HazardEndpoint(Base):
    __tablename__ = "hazard_endpoints"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    substance_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("substances.substance_id", ondelete="CASCADE"), nullable=False)
    source: Mapped[DataSource] = mapped_column(Enum(DataSource, name="data_source"), nullable=False)
    endpoint_type: Mapped[str] = mapped_column(Text, nullable=False)
    hazard_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_value: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    result_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    reliability: Mapped[Reliability | None] = mapped_column(Enum(Reliability, name="reliability"), nullable=True)
    study_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    source_reference: Mapped[str | None] = mapped_column(Text, nullable=True)
    inserted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    substance: Mapped[Substance] = relationship(back_populates="hazard_endpoints")
