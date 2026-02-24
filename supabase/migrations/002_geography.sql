-- =============================================================================
-- 002_geography.sql
-- Reusable updated_at trigger + geography hierarchy tables.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Reusable trigger function — updates updated_at on any row modification.
-- Applied to all tables that carry an updated_at column.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- geography_levels — controlled vocabulary for hierarchy levels
-- pr   = province/territory
-- cd   = census division
-- csd  = census subdivision
-- cma  = census metropolitan area
-- fsa  = forward sortation area
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS geography_levels (
  id          TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  description  TEXT
);

INSERT INTO geography_levels (id, display_name, description) VALUES
  ('country', 'Country',                'Canada as a whole'),
  ('pr',      'Province / Territory',   'Ten provinces and three territories'),
  ('cd',      'Census Division',        'County / regional municipality equivalent'),
  ('csd',     'Census Subdivision',     'Municipality or equivalent'),
  ('cma',     'Census Metropolitan Area','Urban core of 100,000+ population'),
  ('ca',      'Census Agglomeration',   'Urban core of 10,000–100,000 population'),
  ('fsa',     'Forward Sortation Area', 'First three characters of a postal code')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- geographies — canonical geography reference table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS geographies (
  id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  level       TEXT        NOT NULL REFERENCES geography_levels(id),
  sgc_code    TEXT        NOT NULL UNIQUE,   -- Standard Geographical Classification code
  name        TEXT        NOT NULL,
  name_fr     TEXT,                           -- French name
  parent_id   UUID        REFERENCES geographies(id) ON DELETE SET NULL,
  properties  JSONB       NOT NULL DEFAULT '{}',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS geographies_level_idx      ON geographies (level);
CREATE INDEX IF NOT EXISTS geographies_parent_id_idx  ON geographies (parent_id);
CREATE INDEX IF NOT EXISTS geographies_sgc_code_idx   ON geographies (sgc_code);
CREATE INDEX IF NOT EXISTS geographies_name_trgm_idx  ON geographies USING GIN (name gin_trgm_ops);

-- updated_at trigger
CREATE TRIGGER geographies_set_updated_at
  BEFORE UPDATE ON geographies
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- Row-Level Security
-- anon and authenticated users may SELECT; only service_role may mutate.
-- ---------------------------------------------------------------------------
ALTER TABLE geographies ENABLE ROW LEVEL SECURITY;
ALTER TABLE geography_levels ENABLE ROW LEVEL SECURITY;

CREATE POLICY "geographies_select_all"
  ON geographies FOR SELECT
  USING (true);

CREATE POLICY "geographies_insert_service_role"
  ON geographies FOR INSERT
  WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "geographies_update_service_role"
  ON geographies FOR UPDATE
  USING (auth.role() = 'service_role');

CREATE POLICY "geographies_delete_service_role"
  ON geographies FOR DELETE
  USING (auth.role() = 'service_role');

CREATE POLICY "geography_levels_select_all"
  ON geography_levels FOR SELECT
  USING (true);
