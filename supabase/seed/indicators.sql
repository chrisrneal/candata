-- =============================================================================
-- seed/indicators.sql
-- Initial indicator definitions seeded into the indicators table.
-- pipeline workers use these IDs to know where to store values.
-- =============================================================================

INSERT INTO indicators (id, name, source, frequency, unit, description, source_url, statcan_pid, boc_series)
VALUES
  -- -------------------------------------------------------------------------
  -- Statistics Canada — Macroeconomic
  -- -------------------------------------------------------------------------
  (
    'gdp_monthly',
    'Gross Domestic Product (Monthly)',
    'StatCan',
    'monthly',
    'index',
    'Real GDP at basic prices, chained 2017 dollars, seasonally adjusted at annual rates. Table 36-10-0434-01.',
    'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610043401',
    '3610043401',
    NULL
  ),
  (
    'cpi_monthly',
    'Consumer Price Index (Monthly)',
    'StatCan',
    'monthly',
    'index',
    'All-items CPI, 2002=100. Table 18-10-0004-01.',
    'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000401',
    '1810000401',
    NULL
  ),
  (
    'unemployment_rate',
    'Unemployment Rate',
    'StatCan',
    'monthly',
    'percent',
    'Labour Force Survey unemployment rate, seasonally adjusted. Table 14-10-0287-01.',
    'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410028701',
    '1410028701',
    NULL
  ),
  (
    'employment_monthly',
    'Employment (Monthly)',
    'StatCan',
    'monthly',
    'thousands',
    'Number of employed persons, seasonally adjusted, thousands. Table 14-10-0287-01.',
    'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410028701',
    '1410028701',
    NULL
  ),
  (
    'retail_sales_monthly',
    'Retail Trade Sales (Monthly)',
    'StatCan',
    'monthly',
    'dollars',
    'Retail trade, adjusted, seasonally adjusted, CAD millions. Table 20-10-0008-01.',
    'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2010000801',
    '2010000801',
    NULL
  ),

  -- -------------------------------------------------------------------------
  -- Bank of Canada — Interest Rates & FX
  -- -------------------------------------------------------------------------
  (
    'overnight_rate',
    'Bank of Canada Overnight Rate',
    'BoC',
    'daily',
    'percent',
    'Bank of Canada target for the overnight rate.',
    'https://www.bankofcanada.ca/core-functions/monetary-policy/key-interest-rate/',
    NULL,
    'LOOKUPS_V39079'
  ),
  (
    'prime_rate',
    'Canadian Prime Rate',
    'BoC',
    'daily',
    'percent',
    'Prime business loan rate posted by major chartered banks.',
    'https://www.bankofcanada.ca/rates/interest-rates/canadian-interest-rates/',
    NULL,
    'LOOKUPS_V122530'
  ),
  (
    'mortgage_5yr_fixed',
    '5-Year Fixed Mortgage Rate',
    'BoC',
    'weekly',
    'percent',
    'Conventional 5-year mortgage rate posted by major chartered banks.',
    'https://www.bankofcanada.ca/rates/interest-rates/canadian-interest-rates/',
    NULL,
    'LOOKUPS_V80691338'
  ),
  (
    'usdcad',
    'USD/CAD Exchange Rate',
    'BoC',
    'daily',
    'ratio',
    'Noon spot rate, Canadian dollars per US dollar.',
    'https://www.bankofcanada.ca/rates/exchange/daily-exchange-rates/',
    NULL,
    'FXUSDCAD'
  ),

  -- -------------------------------------------------------------------------
  -- CMHC — Housing
  -- -------------------------------------------------------------------------
  (
    'vacancy_rate',
    'Rental Vacancy Rate',
    'CMHC',
    'semi-annual',
    'percent',
    'Rental market survey average vacancy rate for private apartments.',
    'https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data',
    NULL,
    NULL
  ),
  (
    'average_rent',
    'Average Asking Rent',
    'CMHC',
    'semi-annual',
    'dollars',
    'Average asking rent for private apartments by bedroom type.',
    'https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data',
    NULL,
    NULL
  ),
  (
    'housing_starts',
    'Housing Starts',
    'CMHC',
    'monthly',
    'units',
    'Number of new residential dwelling units started.',
    'https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data',
    NULL,
    NULL
  )

ON CONFLICT (id) DO UPDATE SET
  name        = EXCLUDED.name,
  source      = EXCLUDED.source,
  frequency   = EXCLUDED.frequency,
  unit        = EXCLUDED.unit,
  description = EXCLUDED.description,
  source_url  = EXCLUDED.source_url,
  statcan_pid = EXCLUDED.statcan_pid,
  boc_series  = EXCLUDED.boc_series;
