# candata-pipeline

ETL pipeline workers for the candata platform. Extracts Canadian public data from Statistics Canada, Bank of Canada, CMHC, and CanadaBuys, transforms it into normalized schemas, and loads it into Supabase PostgreSQL.

---

## Quick Start

```bash
# Install (shared package must be installed first)
pip install -e ../../shared/python
pip install -e ".[dev]"

# Dry run — extract + transform, no DB writes
python scripts/run_pipeline.py economic-pulse --dry-run

# Full run
python scripts/run_pipeline.py economic-pulse
python scripts/run_pipeline.py all

# Historical backfill
python scripts/backfill.py economic-pulse --from 2015-01-01
```

---

## Package Structure

```
src/candata_pipeline/
├── sources/
│   ├── base.py            # Abstract BaseSource (extract / transform / get_metadata)
│   ├── statcan.py         # StatCan WDS CSV bulk downloads → DuckDB staging
│   ├── bankofcanada.py    # BoC Valet API JSON observations
│   ├── cmhc.py            # CMHC HMIP portal (vacancy, rents, starts)
│   ├── opencanada.py      # Generic CKAN API client for open.canada.ca
│   ├── procurement.py     # Proactive disclosure CSV + CanadaBuys API
│   └── cra_charities.py   # CRA T3010 bulk CSV
│
├── transforms/
│   ├── normalize.py       # GeoNormalizer: raw geo → sgc_code → geography_id
│   ├── time_series.py     # align_to_period_start, fill_gaps, resample_to_frequency
│   └── entities.py        # EntityResolver: fuzzy vendor name deduplication
│
├── loaders/
│   └── supabase_loader.py # SupabaseLoader: batched upsert + pipeline_runs tracking
│
├── pipelines/
│   ├── economic_pulse.py  # StatCan + BoC → indicator_values
│   ├── housing.py         # CMHC → vacancy_rates, average_rents, housing_starts
│   ├── procurement.py     # Contracts + tenders
│   └── trade.py           # StatCan trade table → trade_flows
│
└── utils/
    ├── logging.py         # structlog setup (JSON or console)
    └── retry.py           # @with_retry exponential-backoff decorator
```

---

## Data Sources

| Source | Module | Output Tables | Frequency |
|--------|--------|--------------|-----------|
| Statistics Canada WDS | `statcan.py` | `indicator_values` | Monthly/Quarterly |
| Bank of Canada Valet | `bankofcanada.py` | `indicator_values` | Daily/Weekly |
| CMHC HMIP | `cmhc.py` | `vacancy_rates`, `average_rents`, `housing_starts` | Semi-annual/Monthly |
| open.canada.ca (CKAN) | `opencanada.py` | Various | As published |
| CanadaBuys / proactive disclosure | `procurement.py` | `contracts`, `tenders` | Ongoing |
| CRA T3010 | `cra_charities.py` | `entities` | Monthly |

---

## Pipelines

### `economic-pulse`
Pulls GDP, CPI, Labour Force, Retail Trade from StatCan and all rate/FX series from BoC. Transforms to `indicator_values` schema. Runs StatCan and BoC fetches in parallel via `asyncio.gather`.

```python
from candata_pipeline.pipelines.economic_pulse import run
result = await run(start_date=date(2020, 1, 1))
```

### `housing`
Pulls CMHC rental market survey (vacancy rates, average rents) and housing starts. Loads into the dedicated housing tables rather than `indicator_values`.

```python
from candata_pipeline.pipelines.housing import run
results = await run(year=2023)
# results = {"vacancy_rates": LoadResult, "average_rents": ..., "housing_starts": ...}
```

### `procurement`
Downloads the federal proactive disclosure contracts CSV and active tenders from CanadaBuys. Normalizes department names and deduplicates by contract number.

```python
from candata_pipeline.pipelines.procurement import run
results = await run(datasets=["contracts", "tenders"])
```

### `trade`
Downloads StatCan Table 12-10-0011-01 (international merchandise trade) and loads into `trade_flows`. Values are in CAD (table values × 1000 conversion applied).

```python
from candata_pipeline.pipelines.trade import run
result = await run(start_date=date(2015, 1, 1))
```

---

## Shared Library Integration

All pipeline code imports from `candata_shared`:

```python
from candata_shared.config import settings          # env vars
from candata_shared.db import get_supabase_client   # Supabase client singleton
from candata_shared.db import get_duckdb_connection # DuckDB staging singleton
from candata_shared.models.indicators import Indicator, IndicatorValue
from candata_shared.geo import normalize_statcan_geo
from candata_shared.time_utils import parse_statcan_date
from candata_shared.constants import PROVINCES, INDICATOR_IDS
```

---

## DuckDB Staging

`StatCanSource` caches raw CSV downloads in DuckDB to avoid re-downloading on repeated runs during the same session. The staging file path comes from `settings.duckdb_path` (default: `./data/staging.duckdb`).

The cache is keyed by table PID and has a 24-hour TTL. Use `use_cache=False` to force a fresh download:

```python
df = await source.extract(table_pid="1810000401", use_cache=False)
```

---

## Retry Behaviour

HTTP calls are wrapped with `@with_retry(max_attempts=3, base_delay=N)`:
- Attempt 1 — immediate
- Attempt 2 — after `base_delay` seconds
- Attempt 3 — after `base_delay * 2` seconds

Only `httpx.HTTPError` subclasses trigger retries by default. After 3 failures the original exception is re-raised and the pipeline records a `failure` status in `pipeline_runs`.

---

## Tests

```bash
pytest tests/ -v
pytest tests/test_sources/ -v          # source unit tests (all mocked)
pytest tests/test_transforms/ -v       # transform unit tests (no I/O)
pytest tests/ --co -q                  # list all collected tests
```

All tests mock HTTP via `respx` and mock Supabase via `unittest.mock.MagicMock`. No real network calls or DB connections are made during testing.

---

## Adding a New Source

1. Create `src/candata_pipeline/sources/mysource.py` extending `BaseSource`
2. Implement `extract()`, `transform()`, `get_metadata()`
3. Add a pipeline in `src/candata_pipeline/pipelines/` or integrate into an existing one
4. Add fixture files in `tests/fixtures/` and tests in `tests/test_sources/`
