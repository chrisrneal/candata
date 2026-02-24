---
title: About candata
---

# About candata

candata is a Canadian public data intelligence platform that aggregates, normalizes, and serves economic, housing, procurement, and trade data from official Canadian sources.

## Data Sources

| Source | Data | Frequency |
|--------|------|-----------|
| **Statistics Canada** | GDP, CPI, employment, unemployment, retail sales | Monthly |
| **Bank of Canada** | Overnight rate, prime rate, mortgage rates, USD/CAD | Daily/Weekly |
| **CMHC** | Vacancy rates, average rents, housing starts | Semi-annual/Monthly |
| **CanadaBuys** | Federal contracts and tenders | As published |
| **Statistics Canada** | Trade flows (imports/exports) | Monthly |

## Methodology

- **Collection**: Automated ETL pipelines fetch data from official APIs and open data portals
- **Normalization**: All data is standardized to consistent geography codes (SGC), time periods, and units
- **Storage**: Data is stored in PostgreSQL with full history and versioning
- **Quality**: Automated checks validate data completeness and consistency on each pipeline run

## Update Schedule

| Dataset | Update Frequency | Typical Lag |
|---------|-----------------|-------------|
| Economic indicators | Monthly | 2-3 months |
| Interest rates | Daily | Same day |
| Exchange rates | Daily | Same day |
| Housing data | Semi-annual | 1-2 months |
| Procurement | As published | 1-2 days |
| Trade flows | Monthly | 2-3 months |

## Geography Coverage

- **National**: Canada-wide aggregates
- **Provincial**: All 13 provinces and territories
- **CMA**: 35 census metropolitan areas for housing data
- **Expanding**: Census divisions and subdivisions planned

## Open Data

This public dashboard provides free access to summary views of the data. For full API access with filtering, pagination, and bulk export, see our [pricing plans](/pricing).

---

*Built with Evidence.dev. Data pipeline powered by Polars and DuckDB.*
