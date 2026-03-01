# @candata/shared

Shared TypeScript types and constants for the candata platform. Used by `candata-web` (Next.js dashboard) and any other TypeScript consumer.

## Install

Linked as a workspace dependency — no npm publish required:

```json
{ "@candata/shared": "*" }
```

## Build

```bash
cd shared/typescript
npm run build    # → dist/ (CJS + ESM + .d.ts via tsup)
npm run dev      # watch mode
```

## Usage

```typescript
import type { Indicator, IndicatorValue, ApiResponse } from '@candata/shared';
import type { SavedReport, ReportDefinition, QueryResult } from '@candata/shared';
import { PROVINCES, CMA_CODES, INDICATOR_IDS } from '@candata/shared';
```

## Exported Types

| Module | Types |
|--------|-------|
| `api` | `ApiResponse`, `ApiError`, `PaginationMeta`, `Links` |
| `indicators` | `Indicator`, `IndicatorValue` |
| `housing` | `VacancyRate`, `AverageRent`, `HousingStart` |
| `procurement` | `Contract`, `Tender` |
| `trade` | `TradeFlow` |
| `geography` | `Geography`, `Province`, `CMA`, `CensusDivision`, `FSA`, `GeographyLevel` |
| `entities` | `Entity`, `EntityType`, `EntityRelationship` |
| `reports` | `SavedReport`, `ReportDefinition`, `ReportFilters`, `QueryResult`, `QueryResultColumn`, `QueryResultMeta` |

## Exported Constants

| Constant | Description |
|----------|-------------|
| `PROVINCES` | Map of SGC code → province name |
| `PROVINCE_ABBREVIATIONS` | Map of SGC code → two-letter abbreviation |
| `PROVINCE_NAME_TO_CODE` | Reverse lookup: name → SGC code |
| `ABBREVIATION_TO_CODE` | Reverse lookup: abbreviation → SGC code |
| `CMA_CODES` | Map of CMA name → geoid |
| `INDICATOR_IDS` | All indicator identifiers |
| `FREQUENCIES` | Supported data frequencies |
| `DATA_SOURCES` | Available data source identifiers |
| `TIERS` | API access tiers |
| `GEOGRAPHY_LEVELS` | Supported geography levels |
