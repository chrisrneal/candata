"""
candata_shared.models — Pydantic models matching each database table.

These models are used by:
- packages/pipeline: validate data before writing to Supabase
- packages/api: serialize query results into API responses

All models provide:
  .from_db_row(row: dict) -> Model
  .to_insert_dict() -> dict
"""

from candata_shared.models.entities import Entity, EntityRelationship, EntityType
from candata_shared.models.geography import CensusDivision, CMA, FSA, Geography, Province
from candata_shared.models.housing import AverageRent, HousingStart, VacancyRate
from candata_shared.models.indicators import Indicator, IndicatorValue
from candata_shared.models.procurement import Contract, Tender
from candata_shared.models.trade import TradeFlow

__all__ = [
    "Geography",
    "Province",
    "CMA",
    "CensusDivision",
    "FSA",
    "EntityType",
    "Entity",
    "EntityRelationship",
    "Indicator",
    "IndicatorValue",
    "VacancyRate",
    "AverageRent",
    "HousingStart",
    "Contract",
    "Tender",
    "TradeFlow",
]
