# candata-api

FastAPI backend for the CanData platform — Canadian public data intelligence.

## Setup

```bash
# From the repo root
pip install -e shared/python
pip install -e "packages/api[dev]"
```

## Development

```bash
# Start with auto-reload
bash packages/api/scripts/dev.sh

# Or directly
uvicorn candata_api.app:create_app --factory --reload --port 8000
```

API docs at http://localhost:8000/docs

## Testing

```bash
pytest packages/api/tests -v
```

## Endpoints

### Health
```bash
curl http://localhost:8000/health
# {"status": "ok", "version": "0.1.0"}
```

### Indicators
```bash
# List all indicators
curl http://localhost:8000/v1/indicators

# Get indicator metadata
curl http://localhost:8000/v1/indicators/cpi_monthly

# Get time-series values (national, last 12 months)
curl http://localhost:8000/v1/indicators/cpi_monthly/values

# Filter by geography and date
curl "http://localhost:8000/v1/indicators/cpi_monthly/values?geo=35&start_date=2024-01-01"

# CSV export
curl "http://localhost:8000/v1/indicators/cpi_monthly/values?format=csv" -o cpi.csv
```

### Housing
```bash
# Vacancy rates
curl http://localhost:8000/v1/housing/vacancy-rates

# Average rents filtered by bedroom type
curl "http://localhost:8000/v1/housing/rents?bedroom_type=2br"

# Housing starts
curl http://localhost:8000/v1/housing/starts

# Market summary for a geography
curl http://localhost:8000/v1/housing/market-summary/35
```

### Procurement
```bash
# Search contracts
curl "http://localhost:8000/v1/procurement/contracts?q=infrastructure&department=PSPC"

# Single contract
curl http://localhost:8000/v1/procurement/contracts/{id}

# Vendor contracts
curl http://localhost:8000/v1/procurement/vendors/SNC-Lavalin

# Spending stats
curl "http://localhost:8000/v1/procurement/stats?year=2024"

# Active tenders
curl http://localhost:8000/v1/procurement/tenders
```

### Trade
```bash
# Exports by HS code and partner
curl "http://localhost:8000/v1/trade/exports?hs_code=2709&partner=USA"

# Imports
curl "http://localhost:8000/v1/trade/imports?province=35"

# Trade balance
curl "http://localhost:8000/v1/trade/balance?partner=USA"

# Top commodities
curl "http://localhost:8000/v1/trade/top-commodities?direction=export&year=2024"
```

### Entities
```bash
# Search entities
curl "http://localhost:8000/v1/entities?type=department&q=defence"

# Entity detail
curl http://localhost:8000/v1/entities/{id}

# Entity relationships
curl http://localhost:8000/v1/entities/{id}/relationships
```

### Geography
```bash
# List provinces
curl http://localhost:8000/v1/geo/provinces

# List CMAs
curl http://localhost:8000/v1/geo/cmas

# Geography detail
curl http://localhost:8000/v1/geo/35
```

### Cross-product Search
```bash
curl "http://localhost:8000/v1/search?q=toronto+housing"
```

### Reports (Custom Report Builder)
```bash
# Run an ad-hoc query (no save)
curl -X POST http://localhost:8000/v1/reports/query \
  -H "Authorization: Bearer <jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpi_monthly",
    "group_by": "province",
    "filters": { "provinces": ["ON", "BC"], "date_from": "2022-01", "date_to": "2024-06" }
  }'

# List saved reports
curl http://localhost:8000/v1/reports -H "Authorization: Bearer <jwt>"

# Create a saved report
curl -X POST http://localhost:8000/v1/reports \
  -H "Authorization: Bearer <jwt>" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Ontario CPI Trend",
    "description": "Monthly CPI for Ontario",
    "definition": {
      "metric": "cpi_monthly",
      "group_by": "province",
      "filters": { "provinces": ["ON"], "date_from": "2023-01", "date_to": "2024-06" },
      "viz": "line"
    }
  }'

# Get a saved report
curl http://localhost:8000/v1/reports/{id} -H "Authorization: Bearer <jwt>"

# Update a saved report
curl -X PUT http://localhost:8000/v1/reports/{id} \
  -H "Authorization: Bearer <jwt>" \
  -H "Content-Type: application/json" \
  -d '{ "title": "Updated Title" }'

# Delete a saved report (soft delete)
curl -X DELETE http://localhost:8000/v1/reports/{id} -H "Authorization: Bearer <jwt>"
```

Rate limiting applies to `/v1/reports/query` — a `429` response indicates the user has exceeded their tier's quota.

## Authentication

Three modes, set via `X-API-Key` header or `Authorization: Bearer <jwt>`:

| Tier | Rate Limit | Geographic Access |
|------|-----------|-------------------|
| Free | 100/day | National only |
| Starter | 5,000/month | + Provincial |
| Pro | 50,000/month | + CMA/sub-provincial |
| Business | 500,000/month | Full access |

## Docker

```bash
# Build from repo root
docker build -f packages/api/Dockerfile -t candata-api .

# Run
docker run -p 8000:8000 --env-file .env candata-api
```
