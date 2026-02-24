"""
models/indicators.py — Pydantic models for the indicators and indicator_values tables.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class Indicator(BaseModel):
    """Matches the indicators table row."""

    id: str                          # e.g. "cpi_monthly"
    name: str
    source: str                      # "StatCan", "BoC", "CMHC"
    frequency: str                   # "monthly", "daily", etc.
    unit: str                        # "index", "percent", "dollars", etc.
    description: str | None = None
    source_url: str | None = None
    statcan_pid: str | None = None
    boc_series: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "Indicator":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return self.model_dump(exclude={"created_at", "updated_at"}, exclude_none=True)


class IndicatorValue(BaseModel):
    """
    Matches the indicator_values table row.

    Primary key is (indicator_id, geography_id, ref_date).
    """

    indicator_id: str
    geography_id: UUID
    ref_date: date
    value: Decimal | None = None
    revision_date: date = Field(default_factory=date.today)

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "IndicatorValue":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "indicator_id": self.indicator_id,
            "geography_id": str(self.geography_id),
            "ref_date": self.ref_date.isoformat(),
            "value": float(self.value) if self.value is not None else None,
            "revision_date": self.revision_date.isoformat(),
        }
