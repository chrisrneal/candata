"""
models/trade.py — Pydantic model for the trade_flows table.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class TradeFlow(BaseModel):
    """Matches the trade_flows table row."""

    id: UUID = Field(default_factory=uuid4)
    direction: Literal["import", "export"]
    hs_code: str
    hs_description: str | None = None
    partner_country: str            # ISO 3166-1 alpha-3
    province: str                   # 2-digit SGC code
    ref_date: date
    value_cad: Decimal | None = None
    volume: Decimal | None = None
    volume_unit: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "TradeFlow":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "direction": self.direction,
            "hs_code": self.hs_code,
            "hs_description": self.hs_description,
            "partner_country": self.partner_country,
            "province": self.province,
            "ref_date": self.ref_date.isoformat(),
            "value_cad": float(self.value_cad) if self.value_cad is not None else None,
            "volume": float(self.volume) if self.volume is not None else None,
            "volume_unit": self.volume_unit,
        }
