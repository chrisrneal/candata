---
title: Canadian Housing Market
---

# Canadian Housing Market

Monthly housing starts, completions, and units under construction across
Canadian census metropolitan areas. Data from CMHC.

---

```sql cma_options
select distinct cma_name
from cmhc_housing
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

## Monthly Housing Starts by Dwelling Type

```sql starts_by_type
select
  year,
  month,
  make_date(year, month, 1) as ref_date,
  dwelling_type,
  value as units
from cmhc_housing
where cma_name = '${inputs.selected_cma}'
  and data_type = 'Starts'
  and intended_market = 'Total'
  and dwelling_type in ('Single', 'Semi', 'Row', 'Apartment')
  and make_date(year, month, 1) >= current_date - interval '5 years'
order by ref_date, dwelling_type
```

<LineChart
  data={starts_by_type}
  x=ref_date
  y=units
  series=dwelling_type
  title="Monthly Housing Starts — {inputs.selected_cma}"
  xAxisTitle="Month"
  yAxisTitle="Units"
/>

---

## Annual Completions

```sql annual_completions
select
  year,
  sum(value) as completions
from cmhc_housing
where cma_name = '${inputs.selected_cma}'
  and data_type = 'Completions'
  and dwelling_type = 'Total'
  and intended_market = 'Total'
group by year
order by year
```

<BarChart
  data={annual_completions}
  x=year
  y=completions
  title="Annual Completions — {inputs.selected_cma}"
  xAxisTitle="Year"
  yAxisTitle="Units Completed"
/>

---

## All CMAs — Latest Month Summary

```sql cma_latest_month
with latest as (
  select
    cma_name,
    max(make_date(year, month, 1)) as latest_date
  from cmhc_housing
  where data_type = 'Starts'
    and dwelling_type = 'Total'
    and intended_market = 'Total'
  group by cma_name
),
current_month as (
  select
    h.cma_name,
    h.cma_geoid,
    h.value as starts
  from cmhc_housing h
  join latest l
    on h.cma_name = l.cma_name
    and make_date(h.year, h.month, 1) = l.latest_date
  where h.data_type = 'Starts'
    and h.dwelling_type = 'Total'
    and h.intended_market = 'Total'
),
completions_month as (
  select
    h.cma_name,
    h.value as completions
  from cmhc_housing h
  join latest l
    on h.cma_name = l.cma_name
    and make_date(h.year, h.month, 1) = l.latest_date
  where h.data_type = 'Completions'
    and h.dwelling_type = 'Total'
    and h.intended_market = 'Total'
),
uc_month as (
  select
    h.cma_name,
    h.value as under_construction
  from cmhc_housing h
  join latest l
    on h.cma_name = l.cma_name
    and make_date(h.year, h.month, 1) = l.latest_date
  where h.data_type = 'UnderConstruction'
    and h.dwelling_type = 'Total'
    and h.intended_market = 'Total'
),
prior_year_starts as (
  select
    h.cma_name,
    sum(h.value) as starts_py
  from cmhc_housing h
  join latest l on h.cma_name = l.cma_name
  where h.data_type = 'Starts'
    and h.dwelling_type = 'Total'
    and h.intended_market = 'Total'
    and h.year = extract(year from l.latest_date)::int - 1
  group by h.cma_name
),
current_year_starts as (
  select
    h.cma_name,
    sum(h.value) as starts_cy
  from cmhc_housing h
  join latest l on h.cma_name = l.cma_name
  where h.data_type = 'Starts'
    and h.dwelling_type = 'Total'
    and h.intended_market = 'Total'
    and h.year = extract(year from l.latest_date)::int
  group by h.cma_name
)
select
  cm.cma_name,
  cm.cma_geoid,
  cm.starts,
  co.completions,
  uc.under_construction,
  round(
    (cys.starts_cy - pys.starts_py)::numeric / nullif(pys.starts_py, 0) * 100,
    1
  ) as yoy_change_pct
from current_month cm
left join completions_month co on co.cma_name = cm.cma_name
left join uc_month uc on uc.cma_name = cm.cma_name
left join prior_year_starts pys on pys.cma_name = cm.cma_name
left join current_year_starts cys on cys.cma_name = cm.cma_name
order by cm.starts desc nulls last
```

<DataTable
  data={cma_latest_month}
  rows=20
  search=true
/>

---

*Data sourced from CMHC. Updated monthly. Explore [Affordability Trends](/housing/affordability).*
