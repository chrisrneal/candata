---
title: candata — Canadian Data Explorer
---

# candata Public Dashboard

Free access to Canadian economic, housing, and procurement data.

```sql indicator_count
select count(*) as indicator_count from indicators
```

```sql geo_count
select count(distinct geography_id) as geo_count from indicator_values
```

```sql latest_update
select max(iv.ref_date) as last_updated
from indicator_values iv
```

<BigValue data={indicator_count} value=indicator_count title="Indicators Tracked" />
<BigValue data={geo_count} value=geo_count title="Geographies Covered" />
<BigValue data={latest_update} value=last_updated title="Latest Data Point" />

---

## Explore the Data

- [Economy](/economy) — GDP, CPI, employment, and interest rates
- [Housing](/housing) — Vacancy rates, rents, and housing starts by CMA
- [Trade](/trade) — Import and export flows by commodity and partner
- [About](/about) — Data sources, methodology, and update schedule

---

```sql provinces
select
  g.name,
  g.sgc_code,
  count(distinct iv.indicator_id) as indicators_available
from geographies g
left join indicator_values iv on iv.geography_id = g.id
where g.level = 'pr'
group by g.name, g.sgc_code
order by g.name
```

### Data Availability by Province

<DataTable data={provinces} />

---

*Data sourced from Statistics Canada, Bank of Canada, and CMHC. Updated regularly by the candata pipeline.*
