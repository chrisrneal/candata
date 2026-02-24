-- =============================================================================
-- 009_pipeline_metadata.sql
-- ETL pipeline run tracking and observability.
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
  id                  UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  pipeline_name       TEXT        NOT NULL,   -- e.g. "statcan_cpi", "cmhc_housing"
  source_name         TEXT        NOT NULL,   -- e.g. "StatCan", "BoC", "CMHC"
  status              TEXT        NOT NULL
                      CHECK (status IN ('running', 'success', 'partial_failure', 'failure')),
  records_extracted   INTEGER,                -- rows pulled from source
  records_loaded      INTEGER,                -- rows written to DB (after dedup/validation)
  records_rejected    INTEGER,                -- rows dropped during validation
  started_at          TIMESTAMPTZ,
  completed_at        TIMESTAMPTZ,
  duration_ms         INTEGER GENERATED ALWAYS AS (
    EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000
  ) STORED,
  error_message       TEXT,                   -- last error if status != 'success'
  metadata            JSONB       NOT NULL DEFAULT '{}',  -- arbitrary extra info
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Query patterns: last run per pipeline, all failures, runs by status
CREATE INDEX IF NOT EXISTS pipeline_runs_name_started_idx
  ON pipeline_runs (pipeline_name, started_at DESC);

CREATE INDEX IF NOT EXISTS pipeline_runs_status_idx
  ON pipeline_runs (status);

CREATE INDEX IF NOT EXISTS pipeline_runs_source_idx
  ON pipeline_runs (source_name);

CREATE INDEX IF NOT EXISTS pipeline_runs_started_at_idx
  ON pipeline_runs (started_at DESC);

-- ---------------------------------------------------------------------------
-- Helper: get the most recent completed run for each pipeline
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW latest_pipeline_runs AS
SELECT DISTINCT ON (pipeline_name)
  *
FROM pipeline_runs
WHERE status IN ('success', 'partial_failure', 'failure')
ORDER BY pipeline_name, started_at DESC;

-- ---------------------------------------------------------------------------
-- RLS — authenticated users may read; only service_role may write
-- ---------------------------------------------------------------------------
ALTER TABLE pipeline_runs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "pipeline_runs_select_authenticated"
  ON pipeline_runs FOR SELECT
  USING (auth.role() IN ('authenticated', 'service_role'));

CREATE POLICY "pipeline_runs_insert_service_role"
  ON pipeline_runs FOR INSERT
  WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "pipeline_runs_update_service_role"
  ON pipeline_runs FOR UPDATE
  USING (auth.role() = 'service_role');
