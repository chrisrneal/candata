# candata

Canadian Data Intelligence Platform — monorepo edition

Candata aggregates Statistics Canada, Bank of Canada, CMHC, and federal procurement data into a unified API and dashboard suite. A single Supabase PostgreSQL instance backs all four packages, with shared schema models and utility code used across the Python ETL pipeline, FastAPI backend, Next.js frontend, and Evidence.dev public dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│                                                                 │
│  StatCan SDMX/CSV    Bank of Canada Valet    CMHC Open Data    │
│  buyandsell.gc.ca    StatCan Trade API       More...           │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   packages/pipeline  (Python)                   │
│                                                                 │
│   Extract → DuckDB staging → Transform → Validate → Load       │
│   Pydantic models from candata_shared validate every row.      │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              Supabase PostgreSQL  (shared/supabase/)            │
│                                                                 │
│   geographies · indicators · housing · procurement · trade     │
│   entities · profiles · api_keys · pipeline_runs               │
└──────────┬──────────────────────────────────┬───────────────────┘
           │                                  │
           ▼                                  ▼
┌──────────────────────┐          ┌────────────────────────────┐
│  packages/api        │          │  packages/public-dash      │
│  FastAPI + PostgREST │          │  Evidence.dev              │
│  JWT auth, API keys  │          │  Free public dashboard     │
│  rate limiting       │          │  (reads Supabase directly) │
└──────────┬───────────┘          └────────────────────────────┘
           │
           ▼
┌──────────────────────┐
│  packages/web        │
│  Next.js 14          │
│  Customer dashboard  │
│  Stripe billing      │
└──────────────────────┘
```

---

## Monorepo Layout

```
candata/
├── README.md
├── .gitignore
├── .env.example               # all env vars for all packages
├── docker-compose.yml         # local dev: Supabase + API
│
├── supabase/
│   ├── migrations/            # 001-009 ordered SQL migrations
│   └── seed/                  # province + indicator seed data
│
├── shared/
│   ├── python/                # candata-shared pip package
│   │   └── src/candata_shared/
│   │       ├── config.py      # pydantic Settings
│   │       ├── db.py          # Supabase + DuckDB singletons
│   │       ├── models/        # pydantic models matching DB tables
│   │       ├── geo.py         # province/CMA lookup helpers
│   │       ├── time_utils.py  # StatCan date parsing
│   │       └── constants.py   # codes, IDs, literals
│   │
│   └── typescript/            # @candata/shared npm package
│       └── src/
│           ├── types/         # TS interfaces mirroring pydantic models
│           └── constants.ts   # same codes + IDs as Python
│
├── packages/
│   ├── pipeline/              # Python ETL workers
│   ├── api/                   # FastAPI backend
│   ├── web/                   # Next.js customer dashboard
│   └── public-dash/           # Evidence.dev free dashboard
│
└── scripts/
    ├── setup.sh               # one-time project setup
    ├── dev.sh                 # start all local services
    └── deploy.sh              # deploy all packages
```

---

## Packages

| Package | Language | Purpose |
|---------|----------|---------|
| `packages/pipeline` | Python | Scheduled ETL workers pulling from StatCan, BoC, CMHC, buyandsell |
| `packages/api` | Python (FastAPI) | REST API with JWT + API key auth, rate limiting, Stripe billing hooks |
| `packages/web` | TypeScript (Next.js 14) | Customer-facing dashboard — data explorer, API key management, billing |
| `packages/public-dash` | TypeScript (Evidence.dev) | Free public dashboard embedded at candata.ca/explore |

---

## Shared Code

### Python (`shared/python`)

Installable as `candata-shared`. Used by both `packages/pipeline` and `packages/api`.

```python
from candata_shared.config import settings
from candata_shared.db import get_supabase_client, get_duckdb_connection
from candata_shared.models.indicators import Indicator, IndicatorValue
from candata_shared.models.housing import VacancyRate, AverageRent
from candata_shared.geo import normalize_statcan_geo, province_name_to_code
from candata_shared.constants import INDICATOR_IDS, PROVINCES
```

### TypeScript (`shared/typescript`)

Installable as `@candata/shared`. Used by both `packages/web` and `packages/public-dash`.

```typescript
import type { Indicator, IndicatorValue, ApiResponse } from '@candata/shared';
import { PROVINCES, CMA_CODES, INDICATOR_IDS } from '@candata/shared';
```

---

## Getting Started

### Prerequisites

- Python 3.12+
- Node.js 18+
- [Supabase CLI](https://supabase.com/docs/guides/cli)
- Docker (for local Supabase)

### Setup

```bash
git clone https://github.com/your-org/candata.git
cd candata
bash scripts/setup.sh
```

The setup script will:
1. Install all Python and Node dependencies
2. Start local Supabase via Docker
3. Run all migrations in order
4. Seed province and indicator data

### Local Development

```bash
bash scripts/dev.sh
```

This starts:
- Supabase on `http://localhost:54321`
- Supabase Studio on `http://localhost:54323`
- API on `http://localhost:8000`
- Web on `http://localhost:3000`
- Public dash on `http://localhost:3001`

---

## Environment Variables

Copy `.env.example` to `.env` and fill in real values. See `.env.example` for descriptions.

---

## Database Migrations

Migrations live in `supabase/migrations/` and are applied in filename order:

| File | Contents |
|------|----------|
| `001_extensions.sql` | uuid-ossp, pgcrypto |
| `002_geography.sql` | geographies, updated_at trigger |
| `003_entities.sql` | entities, entity_relationships |
| `004_indicators.sql` | indicators, indicator_values |
| `005_housing.sql` | vacancy_rates, average_rents, housing_starts |
| `006_procurement.sql` | contracts, tenders, full-text search |
| `007_trade.sql` | trade_flows |
| `008_users_billing.sql` | profiles, api_keys, usage_logs |
| `009_pipeline_metadata.sql` | pipeline_runs |

Run migrations:
```bash
supabase db push
```

---

## License

MIT
