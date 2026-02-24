"""
models/housing.py — Pydantic models for CMHC housing market tables.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class VacancyRate(BaseModel):
    """Matches the vacancy_rates table row."""

    id: UUID = Field(default_factory=uuid4)
    geography_id: UUID
    ref_date: date
    bedroom_type: str               # "bachelor", "1br", "2br", "3br+", "total"
    vacancy_rate: Decimal | None = None
    universe: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "VacancyRate":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "geography_id": str(self.geography_id),
            "ref_date": self.ref_date.isoformat(),
            "bedroom_type": self.bedroom_type,
            "vacancy_rate": float(self.vacancy_rate) if self.vacancy_rate is not None else None,
            "universe": self.universe,
        }


class AverageRent(BaseModel):
    """Matches the average_rents table row."""

    id: UUID = Field(default_factory=uuid4)
    geography_id: UUID
    ref_date: date
    bedroom_type: str               # "bachelor", "1br", "2br", "3br+", "total"
    average_rent: Decimal | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "AverageRent":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "geography_id": str(self.geography_id),
            "ref_date": self.ref_date.isoformat(),
            "bedroom_type": self.bedroom_type,
            "average_rent": float(self.average_rent) if self.average_rent is not None else None,
        }


class HousingStart(BaseModel):
    """Matches the housing_starts table row."""

    id: UUID = Field(default_factory=uuid4)
    geography_id: UUID
    ref_date: date
    dwelling_type: str              # "single", "semi", "row", "apartment", "total"
    units: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "HousingStart":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "geography_id": str(self.geography_id),
            "ref_date": self.ref_date.isoformat(),
            "dwelling_type": self.dwelling_type,
            "units": self.units,
        }
