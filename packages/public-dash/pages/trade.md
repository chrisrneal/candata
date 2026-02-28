---
title: Canadian Trade Flows
---

# Canadian Trade Flows

Canadian import and export flows by HS2 product chapter. Data from UN Comtrade.

---

```sql year_options
select distinct period_year as year
from comtrade_flows
order by period_year desc
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

## Top 20 Product Chapters

```sql top_hs2
select
  hs2_code,
  coalesce(hs2_description, 'HS2 ' || hs2_code) as description,
  sum(value_usd) as total_usd
from comtrade_flows
where flow = '${inputs.selected_flow}'
  and period_year = ${inputs.selected_year}
  and hs6_code = ''
group by hs2_code, hs2_description
order by total_usd desc
limit 20
```

<BarChart
  data={top_hs2}
  x=description
  y=total_usd
  swapXY=true
  title="Top 20 HS2 Chapters — {inputs.selected_flow}s {inputs.selected_year}"
  xAxisTitle="HS2 Chapter"
  yAxisTitle="Value (USD)"
/>

---

## Total Imports and Exports Over Time

```sql trade_totals_over_time
select
  period_year,
  flow,
  sum(value_usd) as total_usd
from comtrade_flows
where hs6_code = ''
  and period_year between 2019 and 2023
group by period_year, flow
order by period_year, flow
```

<LineChart
  data={trade_totals_over_time}
  x=period_year
  y=total_usd
  series=flow
  title="Canada Total Trade 2019–2023 (USD)"
  xAxisTitle="Year"
  yAxisTitle="Total Value (USD)"
/>

---

## Trade Summary Table

```sql trade_summary_table
select
  hs2_code,
  coalesce(hs2_description, 'HS2 ' || hs2_code) as description,
  sum(value_usd) as total_usd
from comtrade_flows
where flow = '${inputs.selected_flow}'
  and period_year = ${inputs.selected_year}
  and hs6_code = ''
group by hs2_code, hs2_description
order by total_usd desc
limit 50
```

<DataTable
  data={trade_summary_table}
  rows=15
  search=true
/>

---

*Trade data from UN Comtrade (Canada as reporter, all partners). Explore [Trade by Province](/trade/by-province).*
