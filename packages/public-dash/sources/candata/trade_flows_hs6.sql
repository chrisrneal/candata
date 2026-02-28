-- Pre-aggregate across months and partner countries to reduce data volume.
-- The by-province page only needs (year, province, flow, product) totals.
select
  ref_year,
  province,
  trade_flow,
  napcs_code,
  max(napcs_description) as napcs_description,
  sum(value_cad_millions) as value_cad_millions
from trade_flows_hs6
where ref_year >= extract(year from current_date)::int - 5
group by ref_year, province, trade_flow, napcs_code
order by ref_year desc
