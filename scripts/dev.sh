#!/usr/bin/env bash
# =============================================================================
# scripts/dev.sh — Start all local development services
#
# Run from the repo root:  bash scripts/dev.sh
#
# Starts:
#   - Supabase local stack (PostgreSQL, Auth, PostgREST, Studio)
#   - FastAPI API server (packages/api)
#   - Next.js dev server (packages/web)
#   - Evidence.dev dev server (packages/public-dash)
#
# Each service runs in a background process. Ctrl-C stops all of them.
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${BLUE}[dev]${NC} $*"; }
ok()    { echo -e "${GREEN}[dev]${NC} $*"; }
warn()  { echo -e "${YELLOW}[dev]${NC} $*"; }

# Load .env
if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

PIDS=()

cleanup() {
  echo ""
  info "Shutting down all services..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  ok "All services stopped."
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------
info "Starting Supabase..."
if command -v supabase &>/dev/null; then
  if ! supabase status &>/dev/null 2>&1; then
    supabase start &
    PIDS+=($!)
    sleep 5  # give Supabase time to start
  fi
  ok "  ✓ Supabase running"
  ok "    Studio:    http://localhost:54323"
  ok "    API URL:   http://localhost:54321"
  ok "    DB URL:    postgresql://postgres:postgres@localhost:54322/postgres"
else
  warn "  supabase CLI not found — using docker-compose fallback"
  docker compose up -d supabase-db supabase-kong supabase-studio &>/dev/null || true
  ok "  ✓ Supabase (docker-compose) starting in background"
fi

echo ""

# ---------------------------------------------------------------------------
# FastAPI API
# ---------------------------------------------------------------------------
if [[ -f "packages/api/pyproject.toml" ]] || [[ -f "packages/api/app/main.py" ]]; then
  info "Starting FastAPI (packages/api)..."
  (
    cd packages/api
    _log_level="${LOG_LEVEL:-info}"
    _log_level="${_log_level,,}"  # lowercase for uvicorn
    uvicorn candata_api.app:create_app --factory \
      --host "${API_HOST:-0.0.0.0}" \
      --port "${API_PORT:-8000}" \
      --reload \
      --log-level "$_log_level" \
      2>&1 | sed "s/^/$(printf '\033[0;36m')[api]$(printf '\033[0m') /"
  ) &
  PIDS+=($!)
  ok "  ✓ API starting on http://localhost:${API_PORT:-8000}"
  ok "    Docs:  http://localhost:${API_PORT:-8000}/docs"
else
  warn "  packages/api not ready — skipping API server"
fi

echo ""

# ---------------------------------------------------------------------------
# Next.js web
# ---------------------------------------------------------------------------
if [[ -f "packages/web/package.json" ]]; then
  info "Starting Next.js (packages/web)..."
  (
    cd packages/web
    npm run dev 2>&1 | sed "s/^/$(printf '\033[0;35m')[web]$(printf '\033[0m') /"
  ) &
  PIDS+=($!)
  ok "  ✓ Web starting on http://localhost:3000"
else
  warn "  packages/web not ready — skipping"
fi

echo ""

# ---------------------------------------------------------------------------
# Evidence.dev public dashboard
# ---------------------------------------------------------------------------
if [[ -f "packages/public-dash/package.json" ]]; then
  info "Starting Evidence.dev (packages/public-dash)..."
  (
    cd packages/public-dash
    npm run dev -- --port 3001 2>&1 | sed "s/^/$(printf '\033[1;33m')[dash]$(printf '\033[0m') /"
  ) &
  PIDS+=($!)
  ok "  ✓ Public dash starting on http://localhost:3001"
else
  warn "  packages/public-dash not ready — skipping"
fi

echo ""
info "======================================================"
info " All services started. Press Ctrl-C to stop."
info "======================================================"
echo ""
echo "  Supabase Studio:  http://localhost:54323"
echo "  API + Docs:       http://localhost:${API_PORT:-8000}/docs"
echo "  Web dashboard:    http://localhost:3000"
echo "  Public dashboard: http://localhost:3001"
echo ""

# Wait for all background processes
wait
