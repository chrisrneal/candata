-- =============================================================================
-- 007_trade.sql
-- Statistics Canada trade flows by HS code, province, and partner country.
-- =============================================================================

CREATE TABLE IF NOT EXISTS trade_flows (
  id              UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
  direction       TEXT    NOT NULL CHECK (direction IN ('import', 'export')),
  hs_code         TEXT    NOT NULL,     -- Harmonized System code (2–10 digits)
  hs_description  TEXT,
  hs_chapter      TEXT    GENERATED ALWAYS AS (LEFT(hs_code, 2)) STORED,
  partner_country TEXT    NOT NULL,     -- ISO 3166-1 alpha-3 country code
  province        TEXT    NOT NULL,     -- SGC province code ("35" = Ontario)
  ref_date        DATE    NOT NULL,     -- First day of reference month
  value_cad       NUMERIC,             -- CAD value
  volume          NUMERIC,             -- Physical volume
  volume_unit     TEXT,                -- "kg", "units", etc.
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (direction, hs_code, partner_country, province, ref_date)
);

-- Common query patterns
CREATE INDEX IF NOT EXISTS trade_direction_idx        ON trade_flows (direction);
CREATE INDEX IF NOT EXISTS trade_hs_code_idx          ON trade_flows (hs_code);
CREATE INDEX IF NOT EXISTS trade_hs_chapter_idx       ON trade_flows (hs_chapter);
CREATE INDEX IF NOT EXISTS trade_partner_country_idx  ON trade_flows (partner_country);
CREATE INDEX IF NOT EXISTS trade_province_idx         ON trade_flows (province);
CREATE INDEX IF NOT EXISTS trade_ref_date_idx         ON trade_flows (ref_date DESC);

-- Composite for the most common analytical query: province + date range + direction
CREATE INDEX IF NOT EXISTS trade_province_date_dir_idx
  ON trade_flows (province, ref_date DESC, direction);

CREATE TRIGGER trade_flows_set_updated_at
  BEFORE UPDATE ON trade_flows
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- RLS — public read, service_role write
-- ---------------------------------------------------------------------------
ALTER TABLE trade_flows ENABLE ROW LEVEL SECURITY;

CREATE POLICY "trade_flows_select_all"
  ON trade_flows FOR SELECT USING (true);

CREATE POLICY "trade_flows_insert_service_role"
  ON trade_flows FOR INSERT WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "trade_flows_update_service_role"
  ON trade_flows FOR UPDATE USING (auth.role() = 'service_role');

CREATE POLICY "trade_flows_delete_service_role"
  ON trade_flows FOR DELETE USING (auth.role() = 'service_role');
