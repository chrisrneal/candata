select
  period_year,
  flow,
  hs2_code,
  hs2_description,
  hs6_code,
  value_usd
from comtrade_flows
order by period_year desc
