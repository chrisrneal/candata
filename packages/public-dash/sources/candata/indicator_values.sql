select
  indicator_id,
  geography_id,
  ref_date,
  value
from indicator_values
where ref_date >= current_date - interval '10 years'
order by ref_date desc
