---
title: Trade by Province
---

# Trade by Province

Provincial breakdown of Canadian import and export flows using Statistics
Canada NAPCS/HS6 data (table 12-10-0119-01).

---

```sql year_options
select distinct ref_year as year
from trade_flows_hs6
order by ref_year desc
```

<Dropdown
  data={year_options}
  name=selected_year
  value=year
  title="Year"
  defaultValue=2023
/>

<ButtonGroup name=selected_flow>
  <ButtonGroupItem valueLabel="Exports" value="Export" default />
  <ButtonGroupItem valueLabel="Imports" value="Import" />
</ButtonGroup>

---

## Trade Value by Province

```sql province_totals
select
  province,
  sum(value_cad_millions) as total_cad_millions
from trade_flows_hs6
where trade_flow = '${inputs.selected_flow}'
  and ref_year = ${inputs.selected_year}
group by province
order by total_cad_millions desc
```

<BarChart
  data={province_totals}
  x=province
  y=total_cad_millions
  swapXY=true
  title="{inputs.selected_flow}s by Province — {inputs.selected_year} (CAD millions)"
  xAxisTitle="Province"
  yAxisTitle="Value (CAD millions)"
/>

---

## Top 5 Products per Province

```sql top_products_by_province
with ranked as (
  select
    province,
    napcs_code,
    coalesce(napcs_description, napcs_code) as product,
    sum(value_cad_millions) as total_cad_millions,
    row_number() over (
      partition by province
      order by sum(value_cad_millions) desc
    ) as rank
  from trade_flows_hs6
  where trade_flow = '${inputs.selected_flow}'
    and ref_year = ${inputs.selected_year}
  group by province, napcs_code, napcs_description
)
select
  province,
  rank,
  product,
  round(total_cad_millions::numeric, 1) as value_cad_millions
from ranked
where rank <= 5
order by province, rank
```

<DataTable
  data={top_products_by_province}
  rows=50
  search=true
/>

---

*Data sourced from Statistics Canada table 12-10-0119-01. Province codes follow Statistics Canada SGC conventions.*
*[Back to Trade Flows Overview](/trade)*
