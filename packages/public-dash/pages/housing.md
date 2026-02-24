---
title: Housing Market
---

# Housing Market Overview

Vacancy rates, average rents, and housing starts across Canadian census metropolitan areas. Data from CMHC.

```sql vacancy_overview
select g.name as cma, vr.ref_date, vr.bedroom_type, vr.vacancy_rate
from vacancy_rates vr
join geographies g on g.id = vr.geography_id
where vr.bedroom_type = 'total'
order by vr.ref_date desc
limit 100
```

<DataTable data={vacancy_overview} />

---

## Vacancy Rates by CMA

```sql vacancy_by_cma
select g.name as cma, avg(vr.vacancy_rate) as avg_vacancy_rate
from vacancy_rates vr
join geographies g on g.id = vr.geography_id
where vr.bedroom_type = 'total'
group by g.name
order by avg_vacancy_rate desc
```

<BarChart
  data={vacancy_by_cma}
  x=cma
  y=avg_vacancy_rate
  title="Average Vacancy Rate by CMA"
/>

---

## Average Rents

```sql rent_overview
select g.name as cma, ar.ref_date, ar.bedroom_type, ar.average_rent
from average_rents ar
join geographies g on g.id = ar.geography_id
where ar.bedroom_type = 'total'
order by ar.ref_date desc
limit 100
```

<DataTable data={rent_overview} />

---

## Housing Starts

```sql starts_overview
select g.name as cma, hs.ref_date, hs.dwelling_type, hs.starts
from housing_starts hs
join geographies g on g.id = hs.geography_id
where hs.dwelling_type = 'total'
order by hs.ref_date desc
limit 100
```

<DataTable data={starts_overview} />

---

Explore detailed data for a specific CMA:

```sql cma_list
select distinct g.name as cma
from vacancy_rates vr
join geographies g on g.id = vr.geography_id
order by g.name
```

{#each cma_list as row}

- [{row.cma}](/housing/{row.cma})

{/each}
