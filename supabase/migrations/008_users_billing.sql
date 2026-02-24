-- =============================================================================
-- 008_users_billing.sql
-- User profiles, API keys, and usage logging.
-- Depends on auth.users which is created by Supabase Auth.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- generate_api_key() — generates a random API key with a prefix
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION generate_api_key(prefix TEXT DEFAULT 'sk_live_')
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN prefix || encode(gen_random_bytes(24), 'hex');
END;
$$;

-- ---------------------------------------------------------------------------
-- profiles — one row per auth.users row, extended with billing metadata
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS profiles (
  id                 UUID        PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  email              TEXT,
  full_name          TEXT,
  company            TEXT,
  tier               TEXT        NOT NULL DEFAULT 'free'
                                 CHECK (tier IN ('free', 'starter', 'pro', 'business', 'enterprise')),
  stripe_customer_id TEXT        UNIQUE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS profiles_tier_idx               ON profiles (tier);
CREATE INDEX IF NOT EXISTS profiles_stripe_customer_id_idx ON profiles (stripe_customer_id);

CREATE TRIGGER profiles_set_updated_at
  BEFORE UPDATE ON profiles
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Automatically create a profile row when a new user signs up
CREATE OR REPLACE FUNCTION handle_new_user()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  INSERT INTO public.profiles (id, email, full_name)
  VALUES (
    NEW.id,
    NEW.email,
    NEW.raw_user_meta_data ->> 'full_name'
  )
  ON CONFLICT (id) DO NOTHING;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION handle_new_user();

-- ---------------------------------------------------------------------------
-- api_keys — API keys issued to users
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_keys (
  id               UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id          UUID        NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  key              TEXT        NOT NULL UNIQUE,
  key_prefix       TEXT        NOT NULL,      -- first 12 chars, shown in UI
  name             TEXT,                       -- user-assigned label
  tier             TEXT        NOT NULL DEFAULT 'free'
                               CHECK (tier IN ('free', 'starter', 'pro', 'business', 'enterprise')),
  is_active        BOOLEAN     NOT NULL DEFAULT true,
  last_used_at     TIMESTAMPTZ,
  requests_today   INTEGER     NOT NULL DEFAULT 0,
  requests_month   INTEGER     NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS api_keys_user_id_idx  ON api_keys (user_id);
CREATE INDEX IF NOT EXISTS api_keys_key_idx      ON api_keys (key);
CREATE INDEX IF NOT EXISTS api_keys_active_idx   ON api_keys (is_active) WHERE is_active = true;

CREATE TRIGGER api_keys_set_updated_at
  BEFORE UPDATE ON api_keys
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Reset daily / monthly counters — called from a scheduled job
CREATE OR REPLACE FUNCTION reset_api_key_daily_counts()
RETURNS void LANGUAGE sql SECURITY DEFINER AS $$
  UPDATE api_keys SET requests_today = 0, updated_at = NOW();
$$;

CREATE OR REPLACE FUNCTION reset_api_key_monthly_counts()
RETURNS void LANGUAGE sql SECURITY DEFINER AS $$
  UPDATE api_keys SET requests_month = 0, updated_at = NOW();
$$;

-- ---------------------------------------------------------------------------
-- usage_logs — per-request log for analytics and billing
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS usage_logs (
  id               UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  api_key_id       UUID        REFERENCES api_keys(id) ON DELETE SET NULL,
  endpoint         TEXT        NOT NULL,
  method           TEXT        NOT NULL DEFAULT 'GET',
  status_code      INTEGER,
  response_time_ms INTEGER,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Create initial monthly partitions
CREATE TABLE IF NOT EXISTS usage_logs_2025_01
  PARTITION OF usage_logs FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_02
  PARTITION OF usage_logs FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_03
  PARTITION OF usage_logs FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_04
  PARTITION OF usage_logs FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_05
  PARTITION OF usage_logs FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_06
  PARTITION OF usage_logs FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_07
  PARTITION OF usage_logs FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_08
  PARTITION OF usage_logs FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_09
  PARTITION OF usage_logs FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_10
  PARTITION OF usage_logs FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_11
  PARTITION OF usage_logs FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS usage_logs_2025_12
  PARTITION OF usage_logs FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS usage_logs_2026_01
  PARTITION OF usage_logs FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS usage_logs_2026_02
  PARTITION OF usage_logs FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS usage_logs_2026_03
  PARTITION OF usage_logs FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS usage_logs_default
  PARTITION OF usage_logs DEFAULT;

CREATE INDEX IF NOT EXISTS usage_logs_api_key_id_idx ON usage_logs (api_key_id);
CREATE INDEX IF NOT EXISTS usage_logs_created_at_idx ON usage_logs (created_at DESC);

-- ---------------------------------------------------------------------------
-- RLS — users may only see their own data
-- ---------------------------------------------------------------------------
ALTER TABLE profiles   ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_keys   ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_logs ENABLE ROW LEVEL SECURITY;

-- Profiles
CREATE POLICY "profiles_select_own"
  ON profiles FOR SELECT USING (auth.uid() = id);

CREATE POLICY "profiles_update_own"
  ON profiles FOR UPDATE USING (auth.uid() = id);

-- Service role can do everything (for Stripe webhooks, admin ops)
CREATE POLICY "profiles_service_role_all"
  ON profiles FOR ALL USING (auth.role() = 'service_role');

-- API Keys
CREATE POLICY "api_keys_select_own"
  ON api_keys FOR SELECT USING (auth.uid() = user_id);

CREATE POLICY "api_keys_insert_own"
  ON api_keys FOR INSERT WITH CHECK (auth.uid() = user_id);

CREATE POLICY "api_keys_update_own"
  ON api_keys FOR UPDATE USING (auth.uid() = user_id);

CREATE POLICY "api_keys_delete_own"
  ON api_keys FOR DELETE USING (auth.uid() = user_id);

CREATE POLICY "api_keys_service_role_all"
  ON api_keys FOR ALL USING (auth.role() = 'service_role');

-- Usage logs — users can read logs for their own API keys
CREATE POLICY "usage_logs_select_own"
  ON usage_logs FOR SELECT
  USING (
    api_key_id IN (
      SELECT id FROM api_keys WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "usage_logs_insert_service_role"
  ON usage_logs FOR INSERT WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "usage_logs_service_role_all"
  ON usage_logs FOR ALL USING (auth.role() = 'service_role');
