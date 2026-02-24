-- =============================================================================
-- 004_indicators.sql
-- Generic time-series indicator framework.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- indicators — metadata / catalog for each time series
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS indicators (
  id          TEXT        PRIMARY KEY,   -- e.g. "cpi_monthly", "unemployment_rate"
  name        TEXT        NOT NULL,
  source      TEXT        NOT NULL,      -- "StatCan", "BoC", "CMHC"
  frequency   TEXT        NOT NULL,      -- "daily", "weekly", "monthly", "quarterly", "annual", "semi-annual"
  unit        TEXT        NOT NULL,      -- "index", "percent", "dollars", "thousands", "units", "ratio"
  description TEXT,
  source_url  TEXT,                      -- canonical source documentation URL
  statcan_pid TEXT,                      -- StatCan Product ID (for SDMX retrieval)
  boc_series  TEXT,                      -- Bank of Canada series code
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER indicators_set_updated_at
  BEFORE UPDATE ON indicators
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- indicator_values — one row per (indicator, geography, date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS indicator_values (
  indicator_id    TEXT    NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,
  geography_id    UUID    NOT NULL REFERENCES geographies(id) ON DELETE CASCADE,
  ref_date        DATE    NOT NULL,
  value           NUMERIC,               -- NULL if suppressed / not applicable
  revision_date   DATE    NOT NULL DEFAULT CURRENT_DATE,
  PRIMARY KEY (indicator_id, geography_id, ref_date)
);

-- Query patterns: latest N dates for an indicator+geo, all geos for a date
CREATE INDEX IF NOT EXISTS indicator_values_ref_date_desc_idx
  ON indicator_values (indicator_id, ref_date DESC);

CREATE INDEX IF NOT EXISTS indicator_values_geo_idx
  ON indicator_values (geography_id);

-- ---------------------------------------------------------------------------
-- RLS
-- ---------------------------------------------------------------------------
ALTER TABLE indicators        ENABLE ROW LEVEL SECURITY;
ALTER TABLE indicator_values  ENABLE ROW LEVEL SECURITY;

CREATE POLICY "indicators_select_all"       ON indicators       FOR SELECT USING (true);
CREATE POLICY "indicator_values_select_all" ON indicator_values FOR SELECT USING (true);

CREATE POLICY "indicators_insert_service_role"
  ON indicators FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "indicators_update_service_role"
  ON indicators FOR UPDATE USING (auth.role() = 'service_role');

CREATE POLICY "indicator_values_insert_service_role"
  ON indicator_values FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "indicator_values_update_service_role"
  ON indicator_values FOR UPDATE USING (auth.role() = 'service_role');
CREATE POLICY "indicator_values_delete_service_role"
  ON indicator_values FOR DELETE USING (auth.role() = 'service_role');
