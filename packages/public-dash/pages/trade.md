---
title: Trade Flows
---

# Trade Flows

Canadian import and export data by commodity and partner country.

```sql trade_summary
select direction, hs_chapter, partner_country,
  sum(value_cad) as total_value
from trade_flows
group by direction, hs_chapter, partner_country
order by total_value desc
limit 50
```

<DataTable data={trade_summary} />

---

## Top Export Destinations

```sql exports_by_country
select partner_country, sum(value_cad) as total_exports
from trade_flows
where direction = 'export'
group by partner_country
order by total_exports desc
limit 20
```

<BarChart
  data={exports_by_country}
  x=partner_country
  y=total_exports
  title="Top Export Destinations (CAD)"
/>

---

## Top Import Sources

```sql imports_by_country
select partner_country, sum(value_cad) as total_imports
from trade_flows
where direction = 'import'
group by partner_country
order by total_imports desc
limit 20
```

<BarChart
  data={imports_by_country}
  x=partner_country
  y=total_imports
  title="Top Import Sources (CAD)"
/>

---

## Trade by HS Chapter

```sql trade_by_chapter
select hs_chapter, direction, sum(value_cad) as total_value
from trade_flows
group by hs_chapter, direction
order by total_value desc
limit 30
```

<DataTable data={trade_by_chapter} />
