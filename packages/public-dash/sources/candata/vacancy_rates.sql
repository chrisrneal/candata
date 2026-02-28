select
  geography_id,
  ref_date,
  bedroom_type,
  vacancy_rate
from vacancy_rates
where ref_date >= current_date - interval '10 years'
order by ref_date desc
