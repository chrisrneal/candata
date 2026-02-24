---
title: Trade Flows
---

# Trade Flows

Canadian import and export data by partner country.

```sql trade_summary
select direction, partner_country,
  sum(value_cad) as total_value
from trade_flows
where partner_country != 'ALL'
  and partner_country != 'WLD'
group by direction, partner_country
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
  and partner_country != 'ALL'
  and partner_country != 'WLD'
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
  and partner_country != 'ALL'
  and partner_country != 'WLD'
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

## Trade Balance by Country

```sql trade_balance
select
  partner_country,
  sum(case when direction = 'export' then value_cad else 0 end) as exports,
  sum(case when direction = 'import' then value_cad else 0 end) as imports,
  sum(case when direction = 'export' then value_cad else 0 end) -
    sum(case when direction = 'import' then value_cad else 0 end) as balance
from trade_flows
where partner_country != 'ALL'
  and partner_country != 'WLD'
group by partner_country
order by exports + imports desc
limit 20
```

<DataTable data={trade_balance} />

---

## Monthly Trade Trends

```sql monthly_trade
select ref_date, direction, sum(value_cad) as total_value
from trade_flows
where partner_country = 'ALL' or partner_country = 'WLD'
group by ref_date, direction
order by ref_date
```

<LineChart
  data={monthly_trade}
  x=ref_date
  y=total_value
  series=direction
  title="Monthly Trade Volume (CAD)"
/>
