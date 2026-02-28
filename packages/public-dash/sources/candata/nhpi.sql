select
  cma_name,
  year,
  month,
  house_type,
  index_component,
  index_value
from nhpi
where year >= extract(year from current_date)::int - 5
order by year desc, month desc
