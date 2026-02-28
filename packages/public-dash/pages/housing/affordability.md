---
title: Housing Affordability Trends
---

# Housing Affordability Trends

New Housing Price Index (NHPI) versus housing supply (starts) by CMA.
Rising prices alongside falling starts signal affordability stress.

---

```sql cma_options
select distinct cma_name
from nhpi
order by cma_name
```

<Dropdown
  data={cma_options}
  name=selected_cma
  value=cma_name
  title="Select CMA"
  defaultValue="Toronto"
/>

---

## New Housing Price Index Over Time

```sql nhpi_trend
select
  make_date(year, month, 1) as ref_date,
  index_component,
  index_value
from nhpi
where cma_name = '${inputs.selected_cma}'
  and house_type = 'Total'
  and index_component in ('Total', 'Land', 'Building')
order by ref_date, index_component
```

<LineChart
  data={nhpi_trend}
  x=ref_date
  y=index_value
  series=index_component
  title="New Housing Price Index — {inputs.selected_cma}"
  xAxisTitle="Month"
  yAxisTitle="Index Value (2017=100)"
/>

---

## Monthly Housing Starts (Total)

```sql starts_trend
select
  make_date(year, month, 1) as ref_date,
  value as starts
from cmhc_housing
where cma_name ilike '%' || '${inputs.selected_cma}' || '%'
  and data_type = 'Starts'
  and dwelling_type = 'Total'
  and intended_market = 'Total'
order by ref_date
```

<LineChart
  data={starts_trend}
  x=ref_date
  y=starts
  title="Monthly New Housing Starts — {inputs.selected_cma}"
  xAxisTitle="Month"
  yAxisTitle="Units Started"
/>

---

## Top 10 CMAs by NHPI Increase — Last 3 Years

```sql top_nhpi_increase
with bounds as (
  select
    cma_name,
    min(make_date(year, month, 1)) as min_date,
    max(make_date(year, month, 1)) as max_date
  from nhpi
  where house_type = 'Total'
    and index_component = 'Total'
    and make_date(year, month, 1) >= current_date - interval '3 years'
  group by cma_name
),
earliest as (
  select n.cma_name, n.index_value as index_start
  from nhpi n
  join bounds b
    on n.cma_name = b.cma_name
    and make_date(n.year, n.month, 1) = b.min_date
  where n.house_type = 'Total'
    and n.index_component = 'Total'
),
latest as (
  select n.cma_name, n.index_value as index_end
  from nhpi n
  join bounds b
    on n.cma_name = b.cma_name
    and make_date(n.year, n.month, 1) = b.max_date
  where n.house_type = 'Total'
    and n.index_component = 'Total'
)
select
  e.cma_name,
  round(e.index_start::numeric, 1) as index_3yr_ago,
  round(l.index_end::numeric, 1) as index_latest,
  round((l.index_end - e.index_start)::numeric, 1) as absolute_change,
  round(
    (l.index_end - e.index_start) / nullif(e.index_start, 0) * 100,
    1
  ) as pct_change
from earliest e
join latest l on l.cma_name = e.cma_name
order by pct_change desc nulls last
limit 10
```

<DataTable
  data={top_nhpi_increase}
  rows=10
/>

---

*NHPI data sourced from Statistics Canada table 18-10-0205-01. Housing starts from CMHC.*
*[Back to Housing Market](/housing)*
