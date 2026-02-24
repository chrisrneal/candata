"""
models/geography.py — Pydantic models for the geographies table.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class Geography(BaseModel):
    """Matches the geographies table row exactly."""

    id: UUID | None = None
    level: str
    sgc_code: str
    name: str
    name_fr: str | None = None
    parent_id: UUID | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "Geography":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        d = self.model_dump(exclude={"id", "created_at", "updated_at"}, exclude_none=True)
        if self.id is not None:
            d["id"] = str(self.id)
        if self.parent_id is not None:
            d["parent_id"] = str(self.parent_id)
        return d


class Province(Geography):
    """A province or territory (level='pr')."""

    level: str = "pr"
    abbreviation: str | None = None

    @property
    def sgc_2digit(self) -> str:
        return self.sgc_code.zfill(2)


class CMA(Geography):
    """A Census Metropolitan Area (level='cma')."""

    level: str = "cma"


class CensusDivision(Geography):
    """A Census Division (level='cd')."""

    level: str = "cd"


class FSA(Geography):
    """A Forward Sortation Area — first 3 chars of postal code (level='fsa')."""

    level: str = "fsa"
