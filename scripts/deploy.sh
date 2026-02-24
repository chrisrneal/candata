#!/usr/bin/env bash
# =============================================================================
# scripts/deploy.sh — Deploy all candata packages to production
#
# Usage:  bash scripts/deploy.sh [--skip-migrations] [--skip-web] [--skip-api]
#
# Requires:
#   - SUPABASE_PROJECT_REF  (from .env or environment)
#   - Fly.io CLI (flyctl) for API deployment
#   - Vercel CLI for web deployment
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[deploy]${NC} $*"; }
ok()    { echo -e "${GREEN}[deploy]${NC} $*"; }
warn()  { echo -e "${YELLOW}[deploy]${NC} $*"; }
error() { echo -e "${RED}[deploy]${NC} $*" >&2; exit 1; }

# Parse flags
SKIP_MIGRATIONS=false
SKIP_WEB=false
SKIP_API=false
SKIP_DASH=false

for arg in "$@"; do
  case "$arg" in
    --skip-migrations) SKIP_MIGRATIONS=true ;;
    --skip-web)        SKIP_WEB=true ;;
    --skip-api)        SKIP_API=true ;;
    --skip-dash)       SKIP_DASH=true ;;
  esac
done

# Load .env
if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

echo ""
info "======================================================"
info " candata deploy"
info "======================================================"
echo ""

# ---------------------------------------------------------------------------
# 1. Database migrations
# ---------------------------------------------------------------------------
if [[ "$SKIP_MIGRATIONS" == "false" ]]; then
  info "Running Supabase migrations..."

  if [[ -z "${SUPABASE_PROJECT_REF:-}" ]]; then
    warn "SUPABASE_PROJECT_REF not set — skipping remote migrations."
    warn "Set it in .env or run: supabase link --project-ref <ref>"
  else
    supabase db push --linked
    ok "  ✓ Migrations applied to project $SUPABASE_PROJECT_REF"
  fi
else
  warn "Skipping migrations (--skip-migrations)"
fi

echo ""

# ---------------------------------------------------------------------------
# 2. API (Fly.io)
# ---------------------------------------------------------------------------
if [[ "$SKIP_API" == "false" ]]; then
  info "Deploying API (packages/api)..."

  if ! command -v flyctl &>/dev/null; then
    warn "flyctl not found — skipping API deploy."
    warn "Install: https://fly.io/docs/hands-on/install-flyctl/"
  elif [[ ! -f "packages/api/fly.toml" ]]; then
    warn "packages/api/fly.toml not found — skipping."
    warn "Run 'flyctl launch' inside packages/api to configure."
  else
    flyctl deploy --config packages/api/fly.toml --remote-only
    ok "  ✓ API deployed"
  fi
else
  warn "Skipping API deploy (--skip-api)"
fi

echo ""

# ---------------------------------------------------------------------------
# 3. Web (Vercel)
# ---------------------------------------------------------------------------
if [[ "$SKIP_WEB" == "false" ]]; then
  info "Deploying Web (packages/web)..."

  if ! command -v vercel &>/dev/null; then
    warn "vercel CLI not found — skipping web deploy."
    warn "Install: npm install -g vercel"
  elif [[ ! -f "packages/web/package.json" ]]; then
    warn "packages/web not ready — skipping."
  else
    (cd packages/web && vercel deploy --prod --yes)
    ok "  ✓ Web deployed"
  fi
else
  warn "Skipping web deploy (--skip-web)"
fi

echo ""

# ---------------------------------------------------------------------------
# 4. Public dashboard (Vercel or static host)
# ---------------------------------------------------------------------------
if [[ "$SKIP_DASH" == "false" ]]; then
  info "Deploying public dashboard (packages/public-dash)..."

  if [[ ! -f "packages/public-dash/package.json" ]]; then
    warn "packages/public-dash not ready — skipping."
  else
    (
      cd packages/public-dash
      npm run build
      if command -v vercel &>/dev/null; then
        vercel deploy --prod --yes
        ok "  ✓ Public dashboard deployed"
      else
        warn "vercel CLI not found — build artifacts are in packages/public-dash/build/"
      fi
    )
  fi
else
  warn "Skipping dashboard deploy (--skip-dash)"
fi

echo ""
ok "======================================================"
ok " Deploy complete!"
ok "======================================================"
