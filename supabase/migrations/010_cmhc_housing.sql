-- 010_cmhc_housing.sql — CMHC housing starts/completions/under-construction
-- for all 35 Canadian CMAs, broken down by dwelling type and intended market.

CREATE TABLE IF NOT EXISTS cmhc_housing (
    id              bigserial       PRIMARY KEY,
    cma_name        text            NOT NULL,
    cma_geoid       text            NOT NULL,
    year            integer         NOT NULL,
    month           integer         NOT NULL,
    dwelling_type   text            NOT NULL,   -- Single/Semi/Row/Apartment/Total
    data_type       text            NOT NULL,   -- Starts/Completions/UnderConstruction
    intended_market text            NOT NULL,   -- Freehold/Condo/Rental/Total
    value           integer,
    created_at      timestamptz     NOT NULL DEFAULT now(),

    CONSTRAINT cmhc_housing_unique
        UNIQUE (cma_geoid, year, month, dwelling_type, data_type, intended_market)
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_cmhc_housing_cma_date
    ON cmhc_housing (cma_geoid, year, month);

CREATE INDEX IF NOT EXISTS idx_cmhc_housing_data_type
    ON cmhc_housing (data_type, dwelling_type);

-- RLS: public read, service_role write
ALTER TABLE cmhc_housing ENABLE ROW LEVEL SECURITY;

CREATE POLICY cmhc_housing_public_read ON cmhc_housing
    FOR SELECT USING (true);

CREATE POLICY cmhc_housing_service_write ON cmhc_housing
    FOR ALL USING (auth.role() = 'service_role');
