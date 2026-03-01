# candata

**Canada's public data, unified.**

## What It Is

Candata is a data intelligence platform that aggregates fragmented Canadian government data sources — CMHC, Statistics Canada, UN Comtrade, Teranet, and more — into unified, queryable datasets with a REST API and interactive dashboards. It does for Canadian public data what Bloomberg does for financial markets: consolidates dozens of siloed government portals into a single, normalized data layer.

If you're a policy analyst tracking housing affordability, a real estate firm benchmarking CMA performance, a trade consultancy analyzing cross-border flows, or a journalist fact-checking economic claims — you're currently scraping PDFs, navigating broken SDMX endpoints, and writing one-off parsers. Candata eliminates that work entirely. Query any indicator across any geography and time range with a single API call, explore it visually in the public dashboard, or build custom reports with the built-in report builder.

## Data Products

| Product | Coverage | Granularity | Update Frequency |
|---------|----------|-------------|------------------|
| **Housing Starts & Completions** | All 35 CMAs, 2015–present | Monthly × dwelling type × intended market | Monthly |
| **New Housing Price Index (NHPI)** | Major CMAs | Monthly × land vs. building component | Monthly |
| **Building Permits** | Municipal level | Monthly × structure type × work type | Monthly |
| **Teranet HPI** | Major Canadian markets | Monthly resale house price index | Monthly |
| **Canadian Trade Flows** | National + provincial | Monthly × NAPCS/HS6 product × partner country | Monthly |
| **UN Comtrade Bilateral Trade** | Canada ↔ top 10 partners | Annual × HS2 chapter | Annual |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Pipelines | Python 3.12, Polars, httpx, DuckDB staging |
| Database | Supabase (PostgreSQL) |
| API | FastAPI with JWT + API key auth |
| Customer Dashboard | Next.js 14, Stripe billing |
| Public Dashboard | Evidence.dev |
| Shared Models | Pydantic (Python), TypeScript interfaces |
| Charts & Viz | Recharts, Evidence.dev |
| State & Caching | SWR, @tanstack/react-query |
| Infrastructure | < $75/month |

## Quickstart

### Prerequisites

- Python 3.12+
- Node.js 18+
- [Supabase CLI](https://supabase.com/docs/guides/cli)
- Docker

### Setup

```bash
git clone https://github.com/your-org/candata.git
cd candata
cp .env.example .env        # fill in real values
bash scripts/setup.sh       # installs deps, starts Supabase, runs migrations, seeds data
```

### Run locally

```bash
bash scripts/dev.sh
```

| Service | URL |
|---------|-----|
| API | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Supabase Studio | http://localhost:54323 |
| Web Dashboard | http://localhost:3000 |
| Public Dashboard | http://localhost:3001 |

### Run a pipeline

```bash
# Dry-run CMHC housing data
.venv/bin/python -m candata_pipeline.pipelines.housing --dry-run

# Full load
.venv/bin/python -m candata_pipeline.pipelines.housing
```

### Run tests

```bash
.venv/bin/pytest packages/pipeline/tests/
.venv/bin/pytest packages/api/tests/
```

## API Reference

Interactive OpenAPI docs are served at [`/docs`](http://localhost:8000/docs) when the API is running.

**Endpoint groups:**

- `/housing/*` — CMA-level starts, completions, under construction, affordability trends
- `/trade/*` — Product-level import/export flows, bilateral trade, province breakdown
- `/reports/*` — Custom report builder: create, save, query, and manage report definitions
- `/meta/*` — CMA list, data freshness status

All endpoints return JSON with `Cache-Control: max-age=3600`.

## Monorepo Layout

```
candata/
├── packages/
│   ├── pipeline/        # Python ETL pipelines
│   ├── api/             # FastAPI backend
│   ├── web/             # Next.js customer dashboard
│   └── public-dash/     # Evidence.dev public dashboard
├── shared/
│   ├── python/          # candata-shared (Pydantic models, DB clients, geo helpers)
│   └── typescript/      # @candata/shared (TS interfaces, constants)
├── supabase/
│   └── migrations/      # 001–014 ordered SQL migrations
├── monitoring/          # Data freshness checks and alerting
└── scripts/             # setup.sh, dev.sh, deploy.sh
```

## Roadmap

- [x] Custom report builder with saved definitions & CSV export
- [ ] Census 2021 demographic profiles by CMA
- [ ] StatCan labour force survey integration
- [ ] Bank of Canada interest rate and monetary policy data
- [ ] Real-time pipeline scheduling with Prefect/Dagster
- [ ] Webhook notifications for data updates
- [ ] GraphQL API layer
- [ ] Historical backfill to 2010 for all housing indicators

## License

MIT
