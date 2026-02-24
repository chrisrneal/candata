"""
transforms/entities.py — Entity resolution across procurement and trade sources.

Vendor names from federal contracts and charity names from CRA appear in many
forms: abbreviations, punctuation differences, legal suffixes. This module
normalizes them and attempts fuzzy deduplication so that
"IBM Canada Ltd." and "IBM Canada Limited" resolve to the same entity.

Usage:
    from candata_pipeline.transforms.entities import EntityResolver

    resolver = EntityResolver()
    await resolver.load_entity_cache()

    # Add resolved entity_id to each contract row
    df = resolver.resolve_vendor_names(df, name_col="vendor_name")
    # df now has entity_id and canonical_name columns
"""

from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any

import polars as pl
import structlog

from candata_shared.db import get_supabase_client

log = structlog.get_logger(__name__)

# Legal suffixes to strip before matching
_LEGAL_SUFFIXES = re.compile(
    r"\s+(inc\.?|ltd\.?|limited|corp\.?|corporation|llc|lp|llp|"
    r"incorporated|co\.?|company|enterprises|holdings|group|canada|"
    r"international|solutions|services|technologies?|consulting)\.?\s*$",
    re.IGNORECASE,
)

# Punctuation/whitespace normalizer
_PUNCT = re.compile(r"[^\w\s]")
_SPACE = re.compile(r"\s+")


def normalize_vendor_name(name: str | None) -> str:
    """
    Normalize a vendor/entity name for fuzzy matching.

    Steps:
    1. Unicode decompose + ASCII fold
    2. Lowercase
    3. Strip legal suffixes (Ltd., Inc., Corp., etc.)
    4. Remove punctuation
    5. Collapse whitespace

    Returns empty string for None/empty input.
    """
    if not name:
        return ""

    # Unicode normalize + ASCII fold
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")

    name = name.lower().strip()
    name = _LEGAL_SUFFIXES.sub("", name).strip()
    name = _PUNCT.sub(" ", name)
    name = _SPACE.sub(" ", name).strip()

    return name


class EntityResolver:
    """
    Resolves raw vendor/organization names to canonical entity records.

    Uses a two-stage approach:
    1. Exact match on normalized name (fast dict lookup)
    2. Fuzzy match using SequenceMatcher with a configurable threshold
       (only when exact match fails)

    After resolution, new unmatched names can be registered as new entities.
    """

    def __init__(self, fuzzy_threshold: float = 0.85) -> None:
        self._fuzzy_threshold = fuzzy_threshold
        # normalized_name → {entity_id, canonical_name}
        self._exact_cache: dict[str, dict[str, str]] = {}
        self._loaded = False

    async def load_entity_cache(self, *, entity_type: str = "company") -> None:
        """
        Load existing entities from Supabase into the resolution cache.

        Args:
            entity_type: Filter by entity type (default "company").
        """
        log.info("loading_entity_cache", entity_type=entity_type)
        client = get_supabase_client()
        result = (
            client.table("entities")
            .select("id, name")
            .eq("entity_type", entity_type)
            .execute()
        )
        rows = result.data or []
        self._exact_cache = {
            normalize_vendor_name(row["name"]): {"entity_id": row["id"], "canonical_name": row["name"]}
            for row in rows
        }
        self._loaded = True
        log.info("entity_cache_loaded", count=len(self._exact_cache))

    def resolve_name(self, name: str | None) -> dict[str, str | None]:
        """
        Resolve a single vendor name to a canonical entity.

        Returns:
            dict with keys: entity_id (str|None), canonical_name (str|None),
                            match_type ("exact"|"fuzzy"|"unmatched")
        """
        normalized = normalize_vendor_name(name)
        if not normalized:
            return {"entity_id": None, "canonical_name": None, "match_type": "unmatched"}

        # Exact match
        if normalized in self._exact_cache:
            match = self._exact_cache[normalized]
            return {
                "entity_id": match["entity_id"],
                "canonical_name": match["canonical_name"],
                "match_type": "exact",
            }

        # Fuzzy match
        best_score = 0.0
        best_key: str | None = None
        for key in self._exact_cache:
            score = SequenceMatcher(None, normalized, key).ratio()
            if score > best_score:
                best_score = score
                best_key = key

        if best_score >= self._fuzzy_threshold and best_key:
            match = self._exact_cache[best_key]
            log.debug(
                "entity_fuzzy_match",
                input=name,
                canonical=match["canonical_name"],
                score=round(best_score, 3),
            )
            return {
                "entity_id": match["entity_id"],
                "canonical_name": match["canonical_name"],
                "match_type": "fuzzy",
            }

        return {"entity_id": None, "canonical_name": name, "match_type": "unmatched"}

    def resolve_vendor_names(
        self,
        df: pl.DataFrame,
        name_col: str,
        *,
        entity_id_col: str = "entity_id",
        canonical_col: str = "canonical_name",
    ) -> pl.DataFrame:
        """
        Add entity_id and canonical_name columns to a DataFrame.

        Args:
            df:             Input DataFrame.
            name_col:       Column containing raw vendor/org names.
            entity_id_col:  Output column for entity UUID.
            canonical_col:  Output column for canonical name string.

        Returns:
            DataFrame with two new columns appended.
        """
        resolutions = df[name_col].to_list()
        entity_ids: list[str | None] = []
        canonical_names: list[str | None] = []

        for name in resolutions:
            result = self.resolve_name(name)
            entity_ids.append(result["entity_id"])
            canonical_names.append(result["canonical_name"])

        return df.with_columns(
            pl.Series(entity_id_col, entity_ids, dtype=pl.String),
            pl.Series(canonical_col, canonical_names, dtype=pl.String),
        )

    async def register_new_entities(
        self,
        df: pl.DataFrame,
        name_col: str,
        *,
        entity_type: str = "company",
    ) -> int:
        """
        Insert unmatched names from df as new entity records in Supabase.

        Args:
            df:          DataFrame with entity_id column (from resolve_vendor_names).
            name_col:    Original name column.
            entity_type: Type to assign new entities.

        Returns:
            Number of new entities created.
        """
        if "entity_id" not in df.columns:
            raise ValueError("Call resolve_vendor_names() before register_new_entities()")

        unmatched = df.filter(pl.col("entity_id").is_null())[name_col].unique().to_list()
        if not unmatched:
            return 0

        import uuid
        client = get_supabase_client(service_role=True)
        new_entities = [
            {
                "id": str(uuid.uuid4()),
                "entity_type": entity_type,
                "name": name,
                "external_ids": {},
                "properties": {},
            }
            for name in unmatched
            if name
        ]

        if not new_entities:
            return 0

        client.table("entities").upsert(
            new_entities, on_conflict="entity_type,name"
        ).execute()

        log.info("entities_registered", count=len(new_entities), entity_type=entity_type)
        return len(new_entities)
