select
  geography_id,
  ref_date,
  dwelling_type,
  units
from housing_starts
where ref_date >= current_date - interval '10 years'
order by ref_date desc
