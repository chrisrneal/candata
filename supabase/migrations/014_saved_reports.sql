-- =============================================================================
-- 014_saved_reports.sql
-- Saved report definitions for the custom report builder.
-- =============================================================================

CREATE TABLE IF NOT EXISTS saved_reports (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id      UUID        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  title        TEXT        NOT NULL,
  description  TEXT,
  definition   JSONB       NOT NULL,       -- full report definition JSON
  last_run_at  TIMESTAMPTZ,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at   TIMESTAMPTZ                 -- soft delete
);

-- Only index non-deleted reports per user (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_saved_reports_user
  ON saved_reports (user_id) WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- RLS — users can only see/modify their own reports
-- ---------------------------------------------------------------------------
ALTER TABLE saved_reports ENABLE ROW LEVEL SECURITY;

CREATE POLICY "users_own_reports_select" ON saved_reports
  FOR SELECT USING (auth.uid() = user_id);

CREATE POLICY "users_own_reports_insert" ON saved_reports
  FOR INSERT WITH CHECK (auth.uid() = user_id);

CREATE POLICY "users_own_reports_update" ON saved_reports
  FOR UPDATE USING (auth.uid() = user_id);

CREATE POLICY "users_own_reports_delete" ON saved_reports
  FOR DELETE USING (auth.uid() = user_id);

-- Service role can manage all reports (for admin / pipeline use)
CREATE POLICY "saved_reports_service_role_all" ON saved_reports
  FOR ALL USING (auth.role() = 'service_role');

-- ---------------------------------------------------------------------------
-- Trigger — keep updated_at current
-- ---------------------------------------------------------------------------
CREATE TRIGGER saved_reports_set_updated_at
  BEFORE UPDATE ON saved_reports
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
