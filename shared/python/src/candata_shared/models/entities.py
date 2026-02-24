"""
models/entities.py — Pydantic models for entity_types, entities, entity_relationships.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EntityType(BaseModel):
    """Matches the entity_types table row."""

    id: str
    display_name: str
    description: str | None = None
    properties_schema: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "EntityType":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return self.model_dump()


class Entity(BaseModel):
    """Matches the entities table row."""

    id: UUID = Field(default_factory=uuid4)
    entity_type: str
    name: str
    external_ids: dict[str, Any] = Field(default_factory=dict)
    properties: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "Entity":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "entity_type": self.entity_type,
            "name": self.name,
            "external_ids": self.external_ids,
            "properties": self.properties,
        }


class EntityRelationship(BaseModel):
    """Matches the entity_relationships table row."""

    id: UUID = Field(default_factory=uuid4)
    source_entity_id: UUID
    target_entity_id: UUID
    relationship_type: str
    properties: dict[str, Any] = Field(default_factory=dict)
    valid_from: date | None = None
    valid_to: date | None = None
    created_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "EntityRelationship":
        return cls(**row)

    def to_insert_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "source_entity_id": str(self.source_entity_id),
            "target_entity_id": str(self.target_entity_id),
            "relationship_type": self.relationship_type,
            "properties": self.properties,
            "valid_from": self.valid_from.isoformat() if self.valid_from else None,
            "valid_to": self.valid_to.isoformat() if self.valid_to else None,
        }
