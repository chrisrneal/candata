-- =============================================================================
-- 003_entities.sql
-- Generic entity graph: companies, government departments, persons, etc.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- entity_types — controlled vocabulary of entity categories
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entity_types (
  id                TEXT    PRIMARY KEY,
  display_name      TEXT    NOT NULL,
  description       TEXT,
  properties_schema JSONB   NOT NULL DEFAULT '{}'  -- JSON Schema for properties validation
);

INSERT INTO entity_types (id, display_name, description) VALUES
  ('company',     'Company',              'Federally or provincially incorporated company'),
  ('department',  'Government Department','Federal or provincial government department or agency'),
  ('person',      'Person',              'Named individual (politician, executive, etc.)'),
  ('institution', 'Institution',         'University, hospital, crown corporation, etc.')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- entities — canonical entity records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entities (
  id          UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
  entity_type TEXT    NOT NULL REFERENCES entity_types(id),
  name        TEXT    NOT NULL,
  external_ids JSONB  NOT NULL DEFAULT '{}',  -- {"business_number": "...", "cra_bn": "..."}
  properties  JSONB   NOT NULL DEFAULT '{}',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (entity_type, name)
);

-- Indexes
CREATE INDEX IF NOT EXISTS entities_entity_type_idx   ON entities (entity_type);
CREATE INDEX IF NOT EXISTS entities_name_trgm_idx     ON entities USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS entities_external_ids_gin  ON entities USING GIN (external_ids);
CREATE INDEX IF NOT EXISTS entities_properties_gin    ON entities USING GIN (properties);

CREATE TRIGGER entities_set_updated_at
  BEFORE UPDATE ON entities
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- entity_relationships — directed graph edges
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entity_relationships (
  id                UUID  PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_entity_id  UUID  NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  target_entity_id  UUID  NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  relationship_type TEXT  NOT NULL,  -- e.g. "subsidiary_of", "contracted_by", "board_member_of"
  properties        JSONB NOT NULL DEFAULT '{}',
  valid_from        DATE,
  valid_to          DATE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS entity_rel_source_idx ON entity_relationships (source_entity_id);
CREATE INDEX IF NOT EXISTS entity_rel_target_idx ON entity_relationships (target_entity_id);
CREATE INDEX IF NOT EXISTS entity_rel_type_idx   ON entity_relationships (relationship_type);
CREATE INDEX IF NOT EXISTS entity_rel_valid_idx  ON entity_relationships (valid_from, valid_to);

-- ---------------------------------------------------------------------------
-- RLS
-- ---------------------------------------------------------------------------
ALTER TABLE entity_types           ENABLE ROW LEVEL SECURITY;
ALTER TABLE entities               ENABLE ROW LEVEL SECURITY;
ALTER TABLE entity_relationships   ENABLE ROW LEVEL SECURITY;

CREATE POLICY "entity_types_select_all"        ON entity_types           FOR SELECT USING (true);
CREATE POLICY "entities_select_all"            ON entities               FOR SELECT USING (true);
CREATE POLICY "entity_relationships_select_all" ON entity_relationships  FOR SELECT USING (true);

CREATE POLICY "entities_insert_service_role"   ON entities               FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "entities_update_service_role"   ON entities               FOR UPDATE USING (auth.role() = 'service_role');
CREATE POLICY "entities_delete_service_role"   ON entities               FOR DELETE USING (auth.role() = 'service_role');

CREATE POLICY "entity_rel_insert_service_role" ON entity_relationships   FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "entity_rel_update_service_role" ON entity_relationships   FOR UPDATE USING (auth.role() = 'service_role');
CREATE POLICY "entity_rel_delete_service_role" ON entity_relationships   FOR DELETE USING (auth.role() = 'service_role');
