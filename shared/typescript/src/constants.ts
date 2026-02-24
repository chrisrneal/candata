/**
 * constants.ts — Canonical reference data matching shared/python/constants.py.
 *
 * Keep this file in sync with the Python version.
 */

import type { Frequency, DataSource, Tier } from './types/api.js';
import type { GeographyLevel } from './types/geography.js';

// ---------------------------------------------------------------------------
// Provinces and territories
// ---------------------------------------------------------------------------

/** SGC code → province/territory name */
export const PROVINCES: Readonly<Record<string, string>> = {
  '10': 'Newfoundland and Labrador',
  '11': 'Prince Edward Island',
  '12': 'Nova Scotia',
  '13': 'New Brunswick',
  '24': 'Quebec',
  '35': 'Ontario',
  '46': 'Manitoba',
  '47': 'Saskatchewan',
  '48': 'Alberta',
  '59': 'British Columbia',
  '60': 'Yukon',
  '61': 'Northwest Territories',
  '62': 'Nunavut',
} as const;

/** SGC code → two-letter abbreviation */
export const PROVINCE_ABBREVIATIONS: Readonly<Record<string, string>> = {
  '10': 'NL',
  '11': 'PE',
  '12': 'NS',
  '13': 'NB',
  '24': 'QC',
  '35': 'ON',
  '46': 'MB',
  '47': 'SK',
  '48': 'AB',
  '59': 'BC',
  '60': 'YT',
  '61': 'NT',
  '62': 'NU',
} as const;

/** Province name → SGC code */
export const PROVINCE_NAME_TO_CODE: Readonly<Record<string, string>> = Object.fromEntries(
  Object.entries(PROVINCES).map(([code, name]) => [name, code])
) as Readonly<Record<string, string>>;

/** Two-letter abbreviation → SGC code */
export const ABBREVIATION_TO_CODE: Readonly<Record<string, string>> = Object.fromEntries(
  Object.entries(PROVINCE_ABBREVIATIONS).map(([code, abbr]) => [abbr, code])
) as Readonly<Record<string, string>>;

// ---------------------------------------------------------------------------
// Top CMAs (canonical set matching Python constants)
// ---------------------------------------------------------------------------

export const CMA_CODES: Readonly<Record<string, string>> = {
  '001': 'Toronto',
  '002': 'Montréal',
  '003': 'Vancouver',
  '004': 'Calgary',
  '005': 'Edmonton',
  '006': 'Ottawa-Gatineau',
  '007': 'Winnipeg',
  '008': 'Québec',
  '009': 'Hamilton',
  '010': 'Kitchener-Cambridge-Waterloo',
  '011': 'Abbotsford-Mission',
  '012': 'Halifax',
  '013': 'Oshawa',
  '014': 'London',
  '015': 'Victoria',
  '016': 'St. Catharines-Niagara',
  '017': 'Windsor',
  '018': 'Saskatoon',
  '019': 'Regina',
  '020': 'Sherbrooke',
  '021': "St. John's",
  '022': 'Barrie',
  '023': 'Kelowna',
  '024': 'Abbotsford-Mission',
  '025': 'Greater Sudbury',
  '026': 'Kingston',
  '027': 'Saguenay',
  '028': 'Trois-Rivières',
  '029': 'Guelph',
  '030': 'Moncton',
  '031': 'Brantford',
  '032': 'Thunder Bay',
  '033': 'Saint John',
  '034': 'Peterborough',
  '035': 'Lethbridge',
} as const;

// ---------------------------------------------------------------------------
// Indicator IDs — must match seeds/indicators.sql
// ---------------------------------------------------------------------------

export const INDICATOR_IDS = [
  'gdp_monthly',
  'cpi_monthly',
  'unemployment_rate',
  'employment_monthly',
  'retail_sales_monthly',
  'overnight_rate',
  'prime_rate',
  'mortgage_5yr_fixed',
  'usdcad',
  'vacancy_rate',
  'average_rent',
  'housing_starts',
] as const;

export type IndicatorId = (typeof INDICATOR_IDS)[number];

// ---------------------------------------------------------------------------
// Typed value sets
// ---------------------------------------------------------------------------

export const FREQUENCIES: Frequency[] = [
  'daily',
  'weekly',
  'monthly',
  'quarterly',
  'semi-annual',
  'annual',
];

export const DATA_SOURCES: DataSource[] = [
  'StatCan',
  'BoC',
  'CMHC',
  'CanadaBuys',
];

export const TIERS: Tier[] = [
  'free',
  'starter',
  'pro',
  'business',
  'enterprise',
];

export const GEOGRAPHY_LEVELS: GeographyLevel[] = [
  'country',
  'pr',
  'cd',
  'csd',
  'cma',
  'ca',
  'fsa',
];
