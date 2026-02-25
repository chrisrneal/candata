-- =============================================================================
-- 011_trade_hs6.sql
-- Trade flows at NAPCS / HS6 product level from StatCan table 12-10-0119-01.
-- =============================================================================

CREATE TABLE IF NOT EXISTS trade_flows_hs6 (
  id                 BIGSERIAL   PRIMARY KEY,
  ref_year           INTEGER     NOT NULL,
  ref_month          INTEGER     NOT NULL CHECK (ref_month BETWEEN 1 AND 12),
  province           TEXT        NOT NULL,
  trade_flow         TEXT        NOT NULL CHECK (trade_flow IN ('Import', 'Export')),
  partner_country    TEXT        NOT NULL,
  napcs_code         TEXT        NOT NULL,
  napcs_description  TEXT,
  hs6_code           TEXT,                    -- nullable, from concordance join
  hs6_description    TEXT,                    -- nullable
  value_cad_millions DOUBLE PRECISION,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ref_year, ref_month, province, trade_flow, partner_country, napcs_code)
);

-- Common query patterns
CREATE INDEX IF NOT EXISTS trade_hs6_flow_idx      ON trade_flows_hs6 (trade_flow);
CREATE INDEX IF NOT EXISTS trade_hs6_napcs_idx     ON trade_flows_hs6 (napcs_code);
CREATE INDEX IF NOT EXISTS trade_hs6_hs6_idx       ON trade_flows_hs6 (hs6_code);
CREATE INDEX IF NOT EXISTS trade_hs6_partner_idx   ON trade_flows_hs6 (partner_country);
CREATE INDEX IF NOT EXISTS trade_hs6_province_idx  ON trade_flows_hs6 (province);
CREATE INDEX IF NOT EXISTS trade_hs6_year_idx      ON trade_flows_hs6 (ref_year DESC, ref_month DESC);

-- Composite for analytical queries
CREATE INDEX IF NOT EXISTS trade_hs6_prov_date_idx
  ON trade_flows_hs6 (province, ref_year DESC, ref_month DESC, trade_flow);

-- ---------------------------------------------------------------------------
-- RLS — public read, service_role write
-- ---------------------------------------------------------------------------
ALTER TABLE trade_flows_hs6 ENABLE ROW LEVEL SECURITY;

CREATE POLICY "trade_flows_hs6_select_all"
  ON trade_flows_hs6 FOR SELECT USING (true);

CREATE POLICY "trade_flows_hs6_insert_service_role"
  ON trade_flows_hs6 FOR INSERT WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "trade_flows_hs6_update_service_role"
  ON trade_flows_hs6 FOR UPDATE USING (auth.role() = 'service_role');

CREATE POLICY "trade_flows_hs6_delete_service_role"
  ON trade_flows_hs6 FOR DELETE USING (auth.role() = 'service_role');
