-- =============================================================================
-- 005_housing.sql
-- CMHC housing market data: vacancy rates, average rents, housing starts.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- vacancy_rates — CMHC rental market survey vacancy rates
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vacancy_rates (
  id            UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
  geography_id  UUID    NOT NULL REFERENCES geographies(id) ON DELETE CASCADE,
  ref_date      DATE    NOT NULL,
  bedroom_type  TEXT    NOT NULL,  -- "bachelor", "1br", "2br", "3br+", "total"
  vacancy_rate  NUMERIC,           -- percent (e.g. 2.5 = 2.5%)
  universe      INTEGER,           -- total universe of rental units surveyed
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (geography_id, ref_date, bedroom_type)
);

CREATE INDEX IF NOT EXISTS vacancy_rates_geo_date_idx ON vacancy_rates (geography_id, ref_date DESC);
CREATE INDEX IF NOT EXISTS vacancy_rates_ref_date_idx ON vacancy_rates (ref_date DESC);

CREATE TRIGGER vacancy_rates_set_updated_at
  BEFORE UPDATE ON vacancy_rates
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- average_rents — CMHC average asking rents by bedroom type
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS average_rents (
  id            UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
  geography_id  UUID    NOT NULL REFERENCES geographies(id) ON DELETE CASCADE,
  ref_date      DATE    NOT NULL,
  bedroom_type  TEXT    NOT NULL,  -- "bachelor", "1br", "2br", "3br+", "total"
  average_rent  NUMERIC,           -- CAD per month
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (geography_id, ref_date, bedroom_type)
);

CREATE INDEX IF NOT EXISTS average_rents_geo_date_idx ON average_rents (geography_id, ref_date DESC);
CREATE INDEX IF NOT EXISTS average_rents_ref_date_idx ON average_rents (ref_date DESC);

CREATE TRIGGER average_rents_set_updated_at
  BEFORE UPDATE ON average_rents
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- housing_starts — CMHC housing starts by dwelling type
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS housing_starts (
  id            UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
  geography_id  UUID    NOT NULL REFERENCES geographies(id) ON DELETE CASCADE,
  ref_date      DATE    NOT NULL,
  dwelling_type TEXT    NOT NULL,  -- "single", "semi", "row", "apartment", "total"
  units         INTEGER,           -- number of units started
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (geography_id, ref_date, dwelling_type)
);

CREATE INDEX IF NOT EXISTS housing_starts_geo_date_idx ON housing_starts (geography_id, ref_date DESC);
CREATE INDEX IF NOT EXISTS housing_starts_ref_date_idx ON housing_starts (ref_date DESC);

CREATE TRIGGER housing_starts_set_updated_at
  BEFORE UPDATE ON housing_starts
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- RLS — public read, service_role write
-- ---------------------------------------------------------------------------
ALTER TABLE vacancy_rates   ENABLE ROW LEVEL SECURITY;
ALTER TABLE average_rents   ENABLE ROW LEVEL SECURITY;
ALTER TABLE housing_starts  ENABLE ROW LEVEL SECURITY;

CREATE POLICY "vacancy_rates_select_all"   ON vacancy_rates   FOR SELECT USING (true);
CREATE POLICY "average_rents_select_all"   ON average_rents   FOR SELECT USING (true);
CREATE POLICY "housing_starts_select_all"  ON housing_starts  FOR SELECT USING (true);

CREATE POLICY "vacancy_rates_insert_service_role"  ON vacancy_rates  FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "vacancy_rates_update_service_role"  ON vacancy_rates  FOR UPDATE USING (auth.role() = 'service_role');

CREATE POLICY "average_rents_insert_service_role"  ON average_rents  FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "average_rents_update_service_role"  ON average_rents  FOR UPDATE USING (auth.role() = 'service_role');

CREATE POLICY "housing_starts_insert_service_role" ON housing_starts FOR INSERT WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "housing_starts_update_service_role" ON housing_starts FOR UPDATE USING (auth.role() = 'service_role');
