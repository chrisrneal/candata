-- =============================================================================
-- 001_extensions.sql
-- Enable required PostgreSQL extensions.
-- =============================================================================

-- Universally Unique Identifiers
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Cryptographic functions (used by generate_api_key)
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- PostGIS for geometry/geography types (optional — needed for spatial queries)
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- pg_trgm for fuzzy text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- unaccent for accent-insensitive search
CREATE EXTENSION IF NOT EXISTS unaccent;
