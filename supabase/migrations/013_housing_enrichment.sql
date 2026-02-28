-- 013_housing_enrichment.sql — Additional housing data tables:
--   nhpi              — New Housing Price Index (StatCan 18-10-0205-01)
--   building_permits  — Building permits by municipality (StatCan 34-10-0066-01)
--   teranet_hpi       — Teranet-National Bank House Price Index

-- ---------------------------------------------------------------
-- NHPI: New Housing Price Index by CMA, monthly
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nhpi (
    id                bigserial   PRIMARY KEY,
    cma_name          text        NOT NULL,
    year              integer     NOT NULL,
    month             integer     NOT NULL,
    house_type        text        NOT NULL,   -- Total, Detached
    index_component   text        NOT NULL,   -- Total, Land, Building
    index_value       float8,
    created_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT nhpi_unique
        UNIQUE (cma_name, year, month, house_type, index_component)
);

CREATE INDEX IF NOT EXISTS idx_nhpi_cma_date
    ON nhpi (cma_name, year, month);

ALTER TABLE nhpi ENABLE ROW LEVEL SECURITY;

CREATE POLICY nhpi_public_read ON nhpi
    FOR SELECT USING (true);

CREATE POLICY nhpi_service_write ON nhpi
    FOR ALL USING (auth.role() = 'service_role');

-- ---------------------------------------------------------------
-- Building Permits by Municipality, monthly
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS building_permits (
    id                            bigserial   PRIMARY KEY,
    municipality_name             text        NOT NULL,
    dguid                         text        NOT NULL,
    year                          integer     NOT NULL,
    month                         integer     NOT NULL,
    structure_type                text        NOT NULL,
    work_type                     text        NOT NULL,
    permits_value_cad_thousands   float8,
    created_at                    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT building_permits_unique
        UNIQUE (dguid, year, month, structure_type, work_type)
);

CREATE INDEX IF NOT EXISTS idx_building_permits_geo_date
    ON building_permits (dguid, year, month);

CREATE INDEX IF NOT EXISTS idx_building_permits_structure
    ON building_permits (structure_type, work_type);

ALTER TABLE building_permits ENABLE ROW LEVEL SECURITY;

CREATE POLICY building_permits_public_read ON building_permits
    FOR SELECT USING (true);

CREATE POLICY building_permits_service_write ON building_permits
    FOR ALL USING (auth.role() = 'service_role');

-- ---------------------------------------------------------------
-- Teranet-National Bank House Price Index, monthly
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS teranet_hpi (
    id            bigserial   PRIMARY KEY,
    market_name   text        NOT NULL,
    year          integer     NOT NULL,
    month         integer     NOT NULL,
    hpi_value     float8,
    created_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT teranet_hpi_unique
        UNIQUE (market_name, year, month)
);

CREATE INDEX IF NOT EXISTS idx_teranet_hpi_market_date
    ON teranet_hpi (market_name, year, month);

ALTER TABLE teranet_hpi ENABLE ROW LEVEL SECURITY;

CREATE POLICY teranet_hpi_public_read ON teranet_hpi
    FOR SELECT USING (true);

CREATE POLICY teranet_hpi_service_write ON teranet_hpi
    FOR ALL USING (auth.role() = 'service_role');
