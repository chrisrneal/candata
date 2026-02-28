select
  geography_id,
  ref_date,
  bedroom_type,
  average_rent
from average_rents
where ref_date >= current_date - interval '10 years'
order by ref_date desc
