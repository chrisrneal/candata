#!/usr/bin/env bash
# =============================================================================
# scripts/setup.sh — One-time candata monorepo setup
#
# Run from the repo root:  bash scripts/setup.sh
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()    { echo -e "${BLUE}[setup]${NC} $*"; }
success() { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[setup]${NC} $*"; }
error()   { echo -e "${RED}[setup]${NC} $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Check prerequisites
# ---------------------------------------------------------------------------
info "Checking prerequisites..."

check_cmd() {
  if ! command -v "$1" &>/dev/null; then
    error "'$1' not found. $2"
  fi
  success "  ✓ $1"
}

# Python 3.12+
check_cmd python3 "Install Python 3.12+ from https://python.org"
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)
if [[ "$PYTHON_MAJOR" -lt 3 ]] || [[ "$PYTHON_MAJOR" -eq 3 && "$PYTHON_MINOR" -lt 12 ]]; then
  error "Python 3.12+ required (found $PYTHON_VERSION)"
fi
success "  ✓ Python $PYTHON_VERSION"

# Node.js 18+
check_cmd node "Install Node.js 18+ from https://nodejs.org"
NODE_MAJOR=$(node --version | sed 's/v//' | cut -d. -f1)
if [[ "$NODE_MAJOR" -lt 18 ]]; then
  error "Node.js 18+ required (found $(node --version))"
fi
success "  ✓ Node.js $(node --version)"

check_cmd npm "Install npm (comes with Node.js)"

# Supabase CLI (optional — warn if missing)
if command -v supabase &>/dev/null; then
  success "  ✓ Supabase CLI $(supabase --version)"
  HAS_SUPABASE=true
else
  warn "  ⚠ Supabase CLI not found. Migrations will be skipped."
  warn "    Install: https://supabase.com/docs/guides/cli"
  HAS_SUPABASE=false
fi

# psql (for seed scripts)
if command -v psql &>/dev/null; then
  success "  ✓ psql $(psql --version | head -1)"
  HAS_PSQL=true
else
  warn "  ⚠ psql not found. Seed data will be skipped."
  HAS_PSQL=false
fi

echo ""

# ---------------------------------------------------------------------------
# 2. Environment file
# ---------------------------------------------------------------------------
info "Setting up environment..."

if [[ ! -f ".env" ]]; then
  cp .env.example .env
  success "  ✓ Created .env from .env.example"
  warn "  ⚠ Edit .env and fill in your actual credentials before running services."
else
  success "  ✓ .env already exists"
fi

echo ""

# ---------------------------------------------------------------------------
# 3. Python packages
# ---------------------------------------------------------------------------
info "Installing Python packages..."

# shared
info "  Installing candata-shared..."
pip install -e "shared/python" --quiet --break-system-packages 2>/dev/null \
  || pip install -e "shared/python" --quiet
success "  ✓ candata-shared"

# pipeline
if [[ -f "packages/pipeline/pyproject.toml" ]]; then
  info "  Installing packages/pipeline..."
  pip install -e "packages/pipeline" --quiet --break-system-packages 2>/dev/null \
    || pip install -e "packages/pipeline" --quiet
  success "  ✓ packages/pipeline"
else
  warn "  ⚠ packages/pipeline/pyproject.toml not found — skipping"
fi

# api
if [[ -f "packages/api/pyproject.toml" ]]; then
  info "  Installing packages/api..."
  pip install -e "packages/api" --quiet --break-system-packages 2>/dev/null \
    || pip install -e "packages/api" --quiet
  success "  ✓ packages/api"
else
  warn "  ⚠ packages/api/pyproject.toml not found — skipping"
fi

echo ""

# ---------------------------------------------------------------------------
# 4. Node packages
# ---------------------------------------------------------------------------
info "Installing Node packages..."

install_npm() {
  local dir="$1"
  if [[ -f "$dir/package.json" ]]; then
    info "  npm install in $dir..."
    npm install --prefix "$dir" --silent --legacy-peer-deps
    success "  ✓ $dir"
  else
    warn "  ⚠ $dir/package.json not found — skipping"
  fi
}

install_npm "shared/typescript"
install_npm "packages/web"
install_npm "packages/public-dash"

# Build shared TypeScript
if [[ -f "shared/typescript/package.json" ]]; then
  info "  Building @candata/shared..."
  npm run build --prefix "shared/typescript" --silent
  success "  ✓ @candata/shared built"
fi

echo ""

# ---------------------------------------------------------------------------
# 5. Supabase local stack
# ---------------------------------------------------------------------------
if [[ "$HAS_SUPABASE" == "true" ]]; then
  info "Starting Supabase local stack..."

  if supabase status &>/dev/null 2>&1; then
    success "  ✓ Supabase already running"
  else
    supabase start
    success "  ✓ Supabase started"
  fi

  echo ""

  # Run migrations
  info "Running database migrations..."
  supabase db push
  success "  ✓ Migrations applied"

  echo ""

  # Seed data
  if [[ "$HAS_PSQL" == "true" ]]; then
    info "Seeding database..."

    # Load DATABASE_URL from .env
    if [[ -f ".env" ]]; then
      set -a
      # shellcheck disable=SC1091
      source .env
      set +a
    fi

    DB_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:54322/postgres}"

    psql "$DB_URL" -f supabase/seed/provinces.sql -q
    success "  ✓ Provinces seeded"

    psql "$DB_URL" -f supabase/seed/indicators.sql -q
    success "  ✓ Indicators seeded"
  else
    warn "  ⚠ psql not available — seed data not loaded"
    warn "    Run manually: psql \$DATABASE_URL -f supabase/seed/provinces.sql"
    warn "                  psql \$DATABASE_URL -f supabase/seed/indicators.sql"
  fi
else
  warn "Supabase CLI not available — skipping DB setup."
  warn "Run 'supabase start && supabase db push' manually after installing the CLI."
fi

echo ""

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
success "======================================================"
success " candata setup complete!"
success "======================================================"
echo ""
echo "  Start local development:  bash scripts/dev.sh"
echo "  Studio UI:                http://localhost:54323"
echo "  API:                      http://localhost:8000"
echo "  Web:                      http://localhost:3000"
echo ""
