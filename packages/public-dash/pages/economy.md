---
title: Economic Indicators
---

# Economic Indicators

Overview of key Canadian economic indicators from Statistics Canada and the Bank of Canada.

```sql economic_indicators
select i.name, i.frequency, i.source, i.unit,
  count(iv.ref_date) as data_points,
  min(iv.ref_date) as earliest,
  max(iv.ref_date) as latest
from indicators i
left join indicator_values iv on iv.indicator_id = i.id
group by i.name, i.frequency, i.source, i.unit
order by i.name
```

<DataTable data={economic_indicators} />

---

## CPI Trend

```sql cpi_trend
select iv.ref_date, iv.value
from indicator_values iv
join geographies g on g.id = iv.geography_id
where iv.indicator_id = 'cpi_monthly'
  and g.level = 'country'
order by iv.ref_date
```

<LineChart
  data={cpi_trend}
  x=ref_date
  y=value
  title="Consumer Price Index (Monthly)"
/>

---

## GDP Trend

```sql gdp_trend
select iv.ref_date, iv.value
from indicator_values iv
join geographies g on g.id = iv.geography_id
where iv.indicator_id = 'gdp_monthly'
  and g.level = 'country'
order by iv.ref_date
```

<LineChart
  data={gdp_trend}
  x=ref_date
  y=value
  title="GDP (Monthly)"
/>

---

## Unemployment Rate

```sql unemployment
select iv.ref_date, iv.value
from indicator_values iv
join geographies g on g.id = iv.geography_id
where iv.indicator_id = 'unemployment_rate'
  and g.level = 'country'
order by iv.ref_date
```

<LineChart
  data={unemployment}
  x=ref_date
  y=value
  title="Unemployment Rate"
  yAxisTitle="%"
/>
