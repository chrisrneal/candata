---
title: "Housing: {params.cma}"
---

# Housing Market: {params.cma}

Detailed housing data for {params.cma}.

## Vacancy Rates

```sql vacancy
select vr.ref_date, vr.bedroom_type, vr.vacancy_rate
from vacancy_rates vr
join geographies g on g.id = vr.geography_id
where g.name = '{params.cma}'
order by vr.ref_date desc
```

<LineChart
  data={vacancy}
  x=ref_date
  y=vacancy_rate
  series=bedroom_type
  title="Vacancy Rates Over Time"
/>

<DataTable data={vacancy} />

---

## Average Rents

```sql rents
select ar.ref_date, ar.bedroom_type, ar.average_rent
from average_rents ar
join geographies g on g.id = ar.geography_id
where g.name = '{params.cma}'
order by ar.ref_date desc
```

<LineChart
  data={rents}
  x=ref_date
  y=average_rent
  series=bedroom_type
  title="Average Rents Over Time"
/>

<DataTable data={rents} />

---

## Housing Starts

```sql starts
select hs.ref_date, hs.dwelling_type, hs.starts
from housing_starts hs
join geographies g on g.id = hs.geography_id
where g.name = '{params.cma}'
order by hs.ref_date desc
```

<BarChart
  data={starts}
  x=ref_date
  y=starts
  series=dwelling_type
  title="Housing Starts"
/>

<DataTable data={starts} />
