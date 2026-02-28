-- =============================================================================
-- 012_comtrade_flows.sql
-- UN Comtrade bilateral trade flows at HS2/HS6 level.
-- =============================================================================

CREATE TABLE IF NOT EXISTS comtrade_flows (
  id               BIGSERIAL    PRIMARY KEY,
  period_year      INTEGER      NOT NULL,
  reporter_code    INTEGER      NOT NULL,     -- ISO numeric (124 = Canada)
  partner_code     INTEGER      NOT NULL,
  partner_name     TEXT         NOT NULL,
  hs2_code         TEXT         NOT NULL,
  hs2_description  TEXT,
  hs6_code         TEXT         NOT NULL DEFAULT '',  -- empty string when no HS6 pull
  hs6_description  TEXT,
  flow             TEXT         NOT NULL CHECK (flow IN ('Import', 'Export')),
  value_usd        DOUBLE PRECISION,
  created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Unique constraint for ON CONFLICT upserts via PostgREST
ALTER TABLE comtrade_flows
  ADD CONSTRAINT comtrade_flows_uq
  UNIQUE (period_year, reporter_code, partner_code, hs2_code, flow, hs6_code);

-- Common query patterns
CREATE INDEX IF NOT EXISTS comtrade_year_idx     ON comtrade_flows (period_year);
CREATE INDEX IF NOT EXISTS comtrade_partner_idx  ON comtrade_flows (partner_code);
CREATE INDEX IF NOT EXISTS comtrade_hs2_idx      ON comtrade_flows (hs2_code);
CREATE INDEX IF NOT EXISTS comtrade_hs6_idx      ON comtrade_flows (hs6_code);
CREATE INDEX IF NOT EXISTS comtrade_flow_idx     ON comtrade_flows (flow);

-- Analytical: year + flow + partner for dashboard queries
CREATE INDEX IF NOT EXISTS comtrade_year_flow_partner_idx
  ON comtrade_flows (period_year, flow, partner_code);

-- ---------------------------------------------------------------------------
-- RLS — public read, service_role write
-- ---------------------------------------------------------------------------
ALTER TABLE comtrade_flows ENABLE ROW LEVEL SECURITY;

CREATE POLICY "comtrade_flows_select_all"
  ON comtrade_flows FOR SELECT USING (true);

CREATE POLICY "comtrade_flows_insert_service_role"
  ON comtrade_flows FOR INSERT WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "comtrade_flows_update_service_role"
  ON comtrade_flows FOR UPDATE USING (auth.role() = 'service_role');

CREATE POLICY "comtrade_flows_delete_service_role"
  ON comtrade_flows FOR DELETE USING (auth.role() = 'service_role');
