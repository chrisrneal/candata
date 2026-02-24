"""
models/procurement.py — Pydantic models for the contracts and tenders tables.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Contract(BaseModel):
    """Matches the contracts table row."""

    id: UUID = Field(default_factory=uuid4)
    contract_number: str | None = None
    vendor_name: str
    department: str
    category: str | None = None
    description: str | None = None
    contract_value: Decimal | None = None
    start_date: date | None = None
    end_date: date | None = None
    award_date: date | None = None
    amendment_number: str | None = None
    source_url: str | None = None
    raw_data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "Contract":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "contract_number": self.contract_number,
            "vendor_name": self.vendor_name,
            "department": self.department,
            "category": self.category,
            "description": self.description,
            "contract_value": float(self.contract_value) if self.contract_value is not None else None,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "award_date": self.award_date.isoformat() if self.award_date else None,
            "amendment_number": self.amendment_number,
            "source_url": self.source_url,
            "raw_data": self.raw_data,
        }


class Tender(BaseModel):
    """Matches the tenders table row."""

    id: UUID = Field(default_factory=uuid4)
    tender_number: str | None = None
    title: str
    department: str
    category: str | None = None
    region: str | None = None
    closing_date: date | None = None
    status: str | None = None
    estimated_value: Decimal | None = None
    source_url: str | None = None
    raw_data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "Tender":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "tender_number": self.tender_number,
            "title": self.title,
            "department": self.department,
            "category": self.category,
            "region": self.region,
            "closing_date": self.closing_date.isoformat() if self.closing_date else None,
            "status": self.status,
            "estimated_value": float(self.estimated_value) if self.estimated_value is not None else None,
            "source_url": self.source_url,
            "raw_data": self.raw_data,
        }
