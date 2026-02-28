---
title: Candata — Canadian Public Data Intelligence
---

# Candata

**Canada's public data, unified.**

Candata aggregates housing, trade, and economic data from CMHC, Statistics Canada, UN Comtrade, and Teranet into a single platform. Explore trends across 35 Census Metropolitan Areas, track trade flows by product and partner country, and monitor the indicators that drive the Canadian economy. All data is updated regularly and free to explore.

---

```sql indicator_count
select count(*) as indicator_count from indicators
```

```sql housing_cmas
select count(distinct cma_name) as cma_count from cmhc_housing
```

```sql trade_products
select count(distinct hs2_code) as product_count from comtrade_flows
```

```sql latest_update
select max(iv.ref_date) as last_updated
from indicator_values iv
```

<BigValue data={indicator_count} value=indicator_count title="Indicators Tracked" />
<BigValue data={housing_cmas} value=cma_count title="Housing CMAs" />
<BigValue data={trade_products} value=product_count title="Trade Product Categories" />
<BigValue data={latest_update} value=last_updated title="Latest Data Point" />

---

## Explore the Data

<Grid cols=2>

<Card>

### Housing Market

Housing starts, completions, and units under construction across all 35 CMAs. Monthly data by dwelling type and intended market from 2015 to present.

[Housing Overview &rarr;](/housing)

</Card>

<Card>

### Affordability Trends

New Housing Price Index and housing starts combined to reveal whether rising costs come from land, building, or demand-side pressure.

[Affordability Analysis &rarr;](/housing/affordability)

</Card>

<Card>

### Trade Flows

Canadian imports and exports by product category, partner country, and province. Track trade volumes and year-over-year shifts.

[Trade Overview &rarr;](/trade)

</Card>

<Card>

### Trade by Province

See which provinces are driving Canada's trade activity, broken down by product type and trading partner.

[Provincial Breakdown &rarr;](/trade/by-province)

</Card>

</Grid>

---

## About This Platform

Candata pulls from official Canadian government data sources on a regular schedule, normalizes geography codes and date formats, and serves everything through this dashboard and a REST API. Data is sourced from CMHC, Statistics Canada, UN Comtrade, and Teranet.

[Learn more about our methodology and sources &rarr;](/about)
