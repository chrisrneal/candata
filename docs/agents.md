# Candata — Agent System Prompt Context

## 1. Overview

Candata is a Canadian public data intelligence platform. It aggregates housing, trade, and economic data from CMHC, Statistics Canada, UN Comtrade, and Teranet into a normalized PostgreSQL database, served via a FastAPI REST API and Evidence.dev dashboards.

### What an agent can do

- Query housing trends: starts, completions, under construction, affordability by CMA
- Analyze trade flows: top products, bilateral trade, provincial breakdown
- Monitor data freshness and detect stale sources
- Trigger pipeline runs to refresh data
- Combine endpoints to produce analytical briefings

### Interaction modes

**Read-only API consumer:** Query endpoints, interpret results, produce analysis. No database writes, no pipeline execution. Suitable for analyst-facing agents.

**Pipeline operator:** Full access to run ETL pipelines, check freshness, and ensure data currency. Requires shell access to the candata repository at `/home/chris/appdev/candata` with the Python venv at `.venv/`.

---

## 2. API Interaction Guide

Base URL: `http://localhost:8000` (local) or the deployed host.

All endpoints return JSON with `Cache-Control: max-age=3600`.

### 2.1 Find the fastest-growing CMA by housing starts

**Goal:** Identify which Census Metropolitan Area had the highest year-over-year growth in housing starts over the last 12 months.

**Step 1:** Get the list of all CMAs.

```bash
curl http://localhost:8000/meta/cmas
```

**Response structure:**

```json
[
  {
    "cma_name": "Toronto",
    "cma_geoid": "535",
    "latest_date": "2024-11",
    "record_count": 4320
  }
]
```

**Step 2:** For each CMA, fetch the time series of starts.

```bash
curl "http://localhost:8000/housing/compare?cmas=535,505,462,933,825&metric=starts&dwelling_type=Total&intended_market=Total&from=2023-01&to=2024-12"
```

**Response structure:**

```json
[
  {
    "cma_name": "Toronto",
    "cma_geoid": "535",
    "data": [
      {"year": 2024, "month": 1, "value": 3200},
      {"year": 2024, "month": 2, "value": 2800}
    ]
  }
]
```

**How to interpret:** Sum `value` for the most recent 12 months and the prior 12 months per CMA. Compute `(recent - prior) / prior * 100` for YoY growth. Rank CMAs by this percentage. The `/compare` endpoint accepts up to ~35 comma-separated CMA geoUIDs, so you can batch all CMAs in a few calls.

### 2.2 Compare affordability trends across CMAs

**Goal:** Compare housing price index trends (land vs. building components) across 3 CMAs.

**Endpoint:**

```bash
curl http://localhost:8000/housing/affordability/Toronto
curl http://localhost:8000/housing/affordability/Vancouver
curl http://localhost:8000/housing/affordability/Calgary
```

**Response structure:**

```json
[
  {
    "year": 2024,
    "month": 6,
    "nhpi_composite": 118.2,
    "nhpi_land": 125.4,
    "nhpi_building": 112.1,
    "new_starts_total": 1500
  }
]
```

**How to interpret:** Compare `nhpi_land` growth rates across CMAs to determine where land costs are driving unaffordability. Compare `nhpi_building` to isolate construction cost pressure. Correlate with `new_starts_total` — if starts are falling while the price index rises, supply constraints are worsening.

**Note:** The `cma_name` parameter must match the NHPI table exactly (e.g., "Toronto", "Vancouver", "Calgary"). Use title case city names.

### 2.3 Identify Canada's top export product categories

**Goal:** Find Canada's top 5 export product categories for a given year.

```bash
curl "http://localhost:8000/trade/top-products?flow=Export&year=2023&n=5&source=comtrade"
```

**Response structure:**

```json
[
  {
    "hs2_code": "27",
    "hs2_description": "Mineral fuels, mineral oils",
    "value": 152340.5,
    "prior_value": 189200.1,
    "yoy_change_pct": -19.5
  }
]
```

**How to interpret:** Results are ranked by `value` (USD millions for comtrade source, CAD millions for statcan source). Use `yoy_change_pct` to identify which categories are growing or declining. Set `source=statcan` for NAPCS-based monthly-aggregated data instead of HS2 annual data.

### 2.4 Detect which provinces are driving import growth

**Goal:** Identify which provinces account for the most import activity and how that's changing.

```bash
curl "http://localhost:8000/trade/province-breakdown?year=2023&flow=Import"
curl "http://localhost:8000/trade/province-breakdown?year=2022&flow=Import"
```

**Response structure:**

```json
[
  {
    "province": "35",
    "total_value": 285000.3
  }
]
```

**How to interpret:** Province is identified by SGC code (35 = Ontario, 24 = Quebec, 59 = British Columbia, 48 = Alberta). Compare the two years to compute YoY growth per province. To drill into a specific product, add `&napcs_code=XXXXX`.

**Province code reference:** 10=NL, 11=PE, 12=NS, 13=NB, 24=QC, 35=ON, 46=MB, 47=SK, 48=AB, 59=BC.

### 2.5 Check data freshness before relying on results

**Goal:** Verify all data sources are current before producing an analysis.

```bash
curl http://localhost:8000/meta/data-freshness
```

**Response structure:**

```json
{
  "generated_at": "2024-01-15T10:30:00.000Z",
  "tables": [
    {
      "table": "cmhc_housing",
      "description": "CMHC Housing Starts/Completions by CMA",
      "record_count": 45000,
      "latest_date": "2024-01-01",
      "days_since_latest": 14,
      "max_stale_days": 45,
      "is_stale": false
    }
  ]
}
```

**How to interpret:** If any table has `is_stale: true`, do not rely on that data source for current analysis without first refreshing it. If the endpoint returns HTTP 503, the freshness report has not been generated — run `freshness_check.py` first (see Section 3).

---

## 3. Pipeline Operation Guide

All pipelines are run from the repository root `/home/chris/appdev/candata`. Use the Python venv at `.venv/bin/python`.

### Safe operation sequence

1. **Dry-run first** — validate extraction and transformation without writing to the database.
2. **Scoped test** — run with a narrow filter (single CMA, single year, single province) to verify end-to-end.
3. **Verify** — check the database or freshness endpoint for the expected records.
4. **Full load** — run without filters.

### 3.1 CMHC Housing Pipeline

**What it does:** Ingests housing starts, completions, and units under construction from the CMHC API for all 35 CMAs. Also loads vacancy rates and average rents. Writes to `cmhc_housing`, `vacancy_rates`, `average_rents`, `housing_starts`, and `indicator_values` tables.

**CLI flags:**
- `--dry-run` — transform only, no database writes
- `--cmas` — comma-separated CMA names/IDs to limit scope
- `--start-date` — fetch data from this date onwards (YYYY-MM-DD)

**Dry-run test:**

```bash
.venv/bin/python -m candata_pipeline.pipelines.housing --dry-run --cmas Toronto
```

**Full run:**

```bash
.venv/bin/python -m candata_pipeline.pipelines.housing
```

**Expected runtime:** 5–15 minutes (depends on CMHC API response time).

**Verify success:** Check the freshness endpoint or query the database:

```bash
curl http://localhost:8000/meta/data-freshness | jq '.tables[] | select(.table == "cmhc_housing")'
```

### 3.2 Housing Enrichment Pipeline

**What it does:** Ingests NHPI (New Housing Price Index), building permits, and Teranet HPI. Writes to `nhpi`, `building_permits`, and `teranet_hpi` tables.

**CLI flags:**
- `--dry-run` — transform only, no database writes
- `--source` — one of `nhpi`, `permits`, `teranet`, or `all` (default: `all`)
- `--start-date` — earliest date to ingest (YYYY-MM-DD)

**Dry-run test (NHPI only):**

```bash
.venv/bin/python -m candata_pipeline.pipelines.housing_enrichment --dry-run --source nhpi
```

**Full run:**

```bash
.venv/bin/python -m candata_pipeline.pipelines.housing_enrichment
```

**Expected runtime:** 3–10 minutes. Building permits can take longer due to large CSV downloads.

**Verify success:**

```bash
curl http://localhost:8000/meta/data-freshness | jq '.tables[] | select(.table == "nhpi" or .table == "building_permits")'
```

### 3.3 StatCan Trade HS6 Pipeline

**What it does:** Downloads Canadian international merchandise trade data from Statistics Canada at the NAPCS/HS6 product level by province. Writes to `trade_flows_hs6`. Supports checkpoint-based resume on interruption. Memory-monitored for large bulk CSVs (~2GB+).

**CLI flags:**
- `--dry-run` — transform only, no database writes
- `--from-year` — start year (default: 2019)
- `--to-year` — end year (default: current year)
- `--province` — filter to a single province name

**Dry-run test (single province, single year):**

```bash
.venv/bin/python -m candata_pipeline.pipelines.statcan_trade_hs6 --dry-run --from-year 2023 --to-year 2023 --province Ontario
```

**Full run:**

```bash
.venv/bin/python -m candata_pipeline.pipelines.statcan_trade_hs6
```

**Expected runtime:** 15–45 minutes. Bulk CSV download is the bottleneck. Checkpoint system resumes from last successful chunk if interrupted.

**Verify success:**

```bash
curl http://localhost:8000/meta/data-freshness | jq '.tables[] | select(.table == "trade_flows_hs6")'
```

### 3.4 UN Comtrade Pipeline

**What it does:** Fetches bilateral trade data from the UN Comtrade API for Canada and its key trading partners. Rate-limited to 1 request/second, 500 requests/hour (free tier). Writes to `comtrade_flows`.

**CLI flags:**
- `--dry-run` — transform only, no database writes
- `--level` — `hs2` (chapter level) or `hs6` (6-digit detail)
- `--partners` — comma-separated ISO country codes (default: top 10 partners)
- `--years` — year range or comma-separated years (e.g., `2019-2023` or `2019,2020,2021`)

**Dry-run test (single partner, single year):**

```bash
.venv/bin/python -m candata_pipeline.pipelines.un_comtrade --dry-run --level hs2 --partners 840 --years 2023
```

**Full run:**

```bash
.venv/bin/python -m candata_pipeline.pipelines.un_comtrade
```

**Expected runtime:** 10–30 minutes due to rate limiting. Partner 840 = USA, 156 = China, 276 = Germany.

**Verify success:**

```bash
curl http://localhost:8000/meta/data-freshness | jq '.tables[] | select(.table == "comtrade_flows")'
```

### 3.5 Data Freshness Check

**What it does:** Queries every monitored table's latest date, computes staleness, writes a JSON report to `monitoring/freshness_report.json`, and optionally sends email alerts.

**Run:**

```bash
.venv/bin/python monitoring/freshness_check.py
```

**Expected runtime:** < 30 seconds.

**Verify:** The script outputs a formatted table to stdout and writes the JSON report consumed by `/meta/data-freshness`.

---

## 4. Data Freshness Protocol

**An agent MUST follow this protocol before producing any analysis or answering data-dependent questions.**

### Decision tree

```
1. GET /meta/data-freshness
   │
   ├─ HTTP 503 → Run: .venv/bin/python monitoring/freshness_check.py
   │              Then retry GET /meta/data-freshness
   │
   ├─ HTTP 200 → Parse response
   │   │
   │   ├─ All tables: is_stale == false → Proceed with analysis
   │   │
   │   └─ Any table: is_stale == true → Identify stale tables
   │       │
   │       ├─ cmhc_housing stale → Run housing pipeline
   │       ├─ nhpi stale → Run housing_enrichment --source nhpi
   │       ├─ building_permits stale → Run housing_enrichment --source permits
   │       ├─ trade_flows stale → Run statcan_trade_hs6 pipeline
   │       ├─ trade_flows_hs6 stale → Run statcan_trade_hs6 pipeline
   │       └─ comtrade_flows stale → Run un_comtrade pipeline
   │
   │       After pipeline completes:
   │       → Run freshness_check.py again
   │       → Re-check GET /meta/data-freshness
   │       → Only proceed once all relevant tables show is_stale == false
   │
   └─ HTTP 404 or other error → Log error, do not proceed with stale data
```

### Staleness thresholds

| Table | Max Stale Days | Typical Update Lag |
|-------|---------------|-------------------|
| `cmhc_housing` | 45 | ~30 days after month-end |
| `nhpi` | 45 | ~30 days after month-end |
| `building_permits` | 50 | ~30 days after month-end |
| `trade_flows` | 60 | ~30 days after month-end |
| `trade_flows_hs6` | 60 | ~30 days after month-end |
| `comtrade_flows` | 400 | Annual release, ~6 month lag |

### Important

- Never skip the freshness check. Stale data leads to incorrect analysis.
- If a pipeline operator role is unavailable, warn the user that data may be stale and include the `days_since_latest` value in any output.
- Comtrade data is annual — a `days_since_latest` of 300 days may be normal. Check against the 400-day threshold, not a monthly expectation.

---

## 5. Error Handling Patterns

### API errors

| Error | Meaning | Action |
|-------|---------|--------|
| `404` on `/housing/cma/{geoid}/summary` | CMA geoid not found in database | Fall back to the national total. Query `/housing/compare` with all CMAs and aggregate, or inform the user that this CMA is not tracked. |
| `404` on `/housing/affordability/{name}` | CMA name doesn't match NHPI table | Try common variants (e.g., "St. John's" vs "St. Johns"). Check `/meta/cmas` for exact names. |
| `503` on `/meta/data-freshness` | Freshness report not yet generated | Run `freshness_check.py` first, then retry. |
| `422` on any endpoint | Validation error in query parameters | Read the `detail` field for the specific parameter that failed validation. Fix and retry. |
| Timeout or connection refused | API server not running | Start the API with `bash scripts/dev.sh` or check if the process is running. |

### Pipeline errors

| Error | Meaning | Action |
|-------|---------|--------|
| Non-zero exit code | Pipeline failed during execution | Check stderr for the error message. Re-run with `--dry-run` to isolate whether the failure is in extraction, transformation, or loading. |
| Supabase constraint violation (duplicate key) | Data already exists for this period | Safe to ignore. The upsert conflict columns handle deduplication. The pipeline will skip existing records. |
| Download failure / connection reset | Network interruption during bulk CSV download | Re-run the same command. The checkpoint system (for `statcan_trade_hs6`) resumes from the last successful chunk. Other pipelines restart from the beginning but are idempotent. |
| Rate limit error (HTTP 429) from Comtrade | Exceeded UN Comtrade free tier limits | Wait 1 hour, then re-run. Consider narrowing the scope with `--partners` or `--years` flags. |
| Out of memory | Large CSV exceeding available RAM | For `statcan_trade_hs6`, this should not happen (chunked processing). For other pipelines, try narrowing scope with date or geography filters. |

### General principles

- All pipelines are idempotent. Re-running a pipeline with the same parameters will not create duplicate data.
- Always dry-run before a full run when debugging.
- If a pipeline fails partway through, the data loaded before the failure point is committed. Re-running will fill in the rest.

---

## 6. Example Agent Workflows

### Workflow 1: Housing market briefing for Q1 2024

**Objective:** Produce a briefing identifying the 5 fastest-growing CMAs by housing starts in Q1 2024.

**Step 1: Check data freshness.**

```bash
curl http://localhost:8000/meta/data-freshness
```

Verify `cmhc_housing` has `is_stale: false` and `latest_date` is at or after `2024-03`. If stale, run:

```bash
.venv/bin/python -m candata_pipeline.pipelines.housing
.venv/bin/python monitoring/freshness_check.py
```

**Step 2: Get the full CMA list.**

```bash
curl http://localhost:8000/meta/cmas
```

Extract all `cma_geoid` values. There are approximately 35 CMAs.

**Step 3: Fetch starts data for all CMAs covering Q1 2023 and Q1 2024.**

```bash
curl "http://localhost:8000/housing/compare?cmas=535,505,462,933,825,602,205,305,310,408,421,442,505,515,521,537,539,541,543,550,555,559,568,595,602,725,835,840,915,932,933,935,970&metric=starts&dwelling_type=Total&intended_market=Total&from=2023-01&to=2024-03"
```

**Step 4: Compute YoY growth for each CMA.**

For each CMA in the response:
- Sum `value` for months 2024-01, 2024-02, 2024-03 → `q1_2024`
- Sum `value` for months 2023-01, 2023-02, 2023-03 → `q1_2023`
- Compute `growth_pct = (q1_2024 - q1_2023) / q1_2023 * 100`

**Step 5: Rank and report.**

Sort CMAs by `growth_pct` descending. Take the top 5. For each, fetch the detailed summary:

```bash
curl http://localhost:8000/housing/cma/535/summary
```

**Step 6: Produce the briefing.**

For each of the top 5 CMAs, report:
- CMA name and geoid
- Q1 2024 total starts
- Q1 2023 total starts
- YoY growth percentage
- Latest month snapshot from the CMA summary (starts, completions, under construction)
- Narrative interpretation of the trend

---

### Workflow 2: Provincial import growth from China

**Objective:** Identify which Canadian provinces have the highest year-over-year growth in imports from China.

**Step 1: Check data freshness.**

```bash
curl http://localhost:8000/meta/data-freshness
```

Verify `trade_flows_hs6` has `is_stale: false`. The `trade_flows_hs6` table contains StatCan provincial trade data. If stale, run:

```bash
.venv/bin/python -m candata_pipeline.pipelines.statcan_trade_hs6
.venv/bin/python monitoring/freshness_check.py
```

**Step 2: Get the current and prior year province breakdown for imports.**

```bash
curl "http://localhost:8000/trade/province-breakdown?year=2023&flow=Import"
curl "http://localhost:8000/trade/province-breakdown?year=2022&flow=Import"
```

Note: The `/trade/province-breakdown` endpoint returns all-country totals by province. To isolate China specifically, you need the Comtrade bilateral data.

**Step 3: Get bilateral import data from China.**

```bash
curl "http://localhost:8000/trade/timeseries?hs2=TOTAL&flow=Import&from_year=2022&to_year=2023&partners=China"
```

If the API does not support a TOTAL aggregation, query the top product categories:

```bash
curl "http://localhost:8000/trade/top-products?flow=Import&year=2023&n=100&source=comtrade"
```

Filter results for China as partner. For province-level China imports, use the province-breakdown endpoint with specific NAPCS codes for China-dominant product categories.

**Step 4: Compute YoY growth per province.**

For each province (identified by SGC code):
- `growth_pct = (value_2023 - value_2022) / value_2022 * 100`

**Step 5: Produce the analysis.**

Rank provinces by import growth percentage. For the top 3, identify which product categories (by querying `/trade/top-products` with `source=statcan`) are driving the growth. Map SGC codes to province names: 35=Ontario, 24=Quebec, 59=BC, 48=Alberta, 46=Manitoba, 47=Saskatchewan, 12=Nova Scotia, 13=New Brunswick, 10=Newfoundland, 11=PEI.

---

### Workflow 3: Pre-demo data freshness verification

**Objective:** Verify all data is fresh, run any stale pipelines, and confirm the platform is ready for a client demo.

**Step 1: Generate the freshness report.**

```bash
.venv/bin/python monitoring/freshness_check.py
```

**Step 2: Check the freshness endpoint.**

```bash
curl http://localhost:8000/meta/data-freshness
```

**Step 3: Identify stale tables.**

Parse the response. For each table where `is_stale: true`, map to the appropriate pipeline:

| Stale Table | Pipeline Command |
|-------------|-----------------|
| `cmhc_housing` | `.venv/bin/python -m candata_pipeline.pipelines.housing` |
| `nhpi` | `.venv/bin/python -m candata_pipeline.pipelines.housing_enrichment --source nhpi` |
| `building_permits` | `.venv/bin/python -m candata_pipeline.pipelines.housing_enrichment --source permits` |
| `trade_flows_hs6` | `.venv/bin/python -m candata_pipeline.pipelines.statcan_trade_hs6` |
| `comtrade_flows` | `.venv/bin/python -m candata_pipeline.pipelines.un_comtrade` |

**Step 4: Run stale pipelines with dry-run first.**

For each stale table, dry-run:

```bash
.venv/bin/python -m candata_pipeline.pipelines.housing --dry-run
```

If dry-run succeeds (exit code 0), proceed with full run:

```bash
.venv/bin/python -m candata_pipeline.pipelines.housing
```

**Step 5: Re-check freshness after all pipelines complete.**

```bash
.venv/bin/python monitoring/freshness_check.py
curl http://localhost:8000/meta/data-freshness
```

**Step 6: Verify all tables show `is_stale: false`.**

If any table remains stale after pipeline completion, investigate:
- Check pipeline stdout/stderr for errors
- Verify the source data has been updated (some government sources have publication delays)
- If the source itself hasn't published new data, this is expected — note it for the demo

**Step 7: Smoke-test key endpoints.**

```bash
curl http://localhost:8000/housing/cma/535/summary
curl "http://localhost:8000/trade/top-products?flow=Export&year=2023&n=5"
curl http://localhost:8000/meta/cmas
```

Verify each returns HTTP 200 with non-empty data. The platform is ready for the demo.
