-- =============================================================================
-- 006_procurement.sql
-- Federal procurement data: awarded contracts and open tenders.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- contracts — awarded federal contracts from buyandsell.gc.ca / CanadaBuys
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contracts (
  id              UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
  contract_number TEXT    UNIQUE,
  vendor_name     TEXT    NOT NULL,
  department      TEXT    NOT NULL,
  category        TEXT,                  -- GSIN / commodity code category
  description     TEXT,
  contract_value  NUMERIC,              -- CAD
  start_date      DATE,
  end_date        DATE,
  award_date      DATE,
  amendment_number TEXT,               -- NULL for original, "1", "2" for amendments
  source_url      TEXT,
  raw_data        JSONB   NOT NULL DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Lookup indexes
CREATE INDEX IF NOT EXISTS contracts_vendor_idx        ON contracts (vendor_name);
CREATE INDEX IF NOT EXISTS contracts_department_idx    ON contracts (department);
CREATE INDEX IF NOT EXISTS contracts_award_date_idx    ON contracts (award_date DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS contracts_value_idx         ON contracts (contract_value DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS contracts_category_idx      ON contracts (category);

-- Full-text search index (English + French combined via concat)
CREATE INDEX IF NOT EXISTS contracts_fts_idx ON contracts
  USING GIN (
    to_tsvector(
      'english',
      coalesce(vendor_name, '') || ' ' ||
      coalesce(department, '') || ' ' ||
      coalesce(description, '')
    )
  );

-- Trigram index for partial name matches
CREATE INDEX IF NOT EXISTS contracts_vendor_trgm_idx ON contracts USING GIN (vendor_name gin_trgm_ops);

CREATE TRIGGER contracts_set_updated_at
  BEFORE UPDATE ON contracts
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- tenders — open / closed solicitations
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenders (
  id              UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
  tender_number   TEXT    UNIQUE,
  title           TEXT    NOT NULL,
  department      TEXT    NOT NULL,
  category        TEXT,
  region          TEXT,                  -- delivery / service region
  closing_date    DATE,
  status          TEXT,                  -- "open", "closed", "cancelled", "awarded"
  estimated_value NUMERIC,
  source_url      TEXT,
  raw_data        JSONB   NOT NULL DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS tenders_department_idx   ON tenders (department);
CREATE INDEX IF NOT EXISTS tenders_closing_date_idx ON tenders (closing_date DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS tenders_status_idx       ON tenders (status);
CREATE INDEX IF NOT EXISTS tenders_category_idx     ON tenders (category);
CREATE INDEX IF NOT EXISTS tenders_fts_idx ON tenders
  USING GIN (
    to_tsvector(
      'english',
      coalesce(title, '') || ' ' ||
      coalesce(department, '') || ' ' ||
      coalesce(category, '')
    )
  );

CREATE TRIGGER tenders_set_updated_at
  BEFORE UPDATE ON tenders
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- RLS — public read, service_role write
-- ---------------------------------------------------------------------------
ALTER TABLE contracts ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenders   ENABLE ROW LEVEL SECURITY;

CREATE POLICY "contracts_select_all" ON contracts FOR SELECT USING (true);
CREATE POLICY "tenders_select_all"   ON tenders   FOR SELECT USING (true);

CREATE POLICY "contracts_insert_service_role" ON contracts FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "contracts_update_service_role" ON contracts FOR UPDATE USING (auth.role() = 'service_role');
CREATE POLICY "contracts_delete_service_role" ON contracts FOR DELETE USING (auth.role() = 'service_role');

CREATE POLICY "tenders_insert_service_role"   ON tenders FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "tenders_update_service_role"   ON tenders FOR UPDATE USING (auth.role() = 'service_role');
CREATE POLICY "tenders_delete_service_role"   ON tenders FOR DELETE USING (auth.role() = 'service_role');
