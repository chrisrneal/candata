select
  cma_name,
  cma_geoid,
  year,
  month,
  dwelling_type,
  data_type,
  intended_market,
  value
from cmhc_housing
where year >= extract(year from current_date)::int - 5
order by year desc, month desc
