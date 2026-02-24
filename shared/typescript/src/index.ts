/**
 * @candata/shared — TypeScript types and constants for the candata platform.
 *
 * Usage:
 *   import type { Indicator, IndicatorValue, ApiResponse } from '@candata/shared';
 *   import { PROVINCES, INDICATOR_IDS } from '@candata/shared';
 */

// Types
export type { ApiResponse, ApiError, PaginationMeta, Links } from './types/api';
export type { Indicator, IndicatorValue } from './types/indicators';
export type { VacancyRate, AverageRent, HousingStart } from './types/housing';
export type { Contract, Tender } from './types/procurement';
export type { TradeFlow } from './types/trade';
export type {
  Geography,
  Province,
  CMA,
  CensusDivision,
  FSA,
  GeographyLevel,
} from './types/geography';
export type {
  Entity,
  EntityType,
  EntityRelationship,
} from './types/entities';

// Constants
export {
  PROVINCES,
  PROVINCE_ABBREVIATIONS,
  PROVINCE_NAME_TO_CODE,
  ABBREVIATION_TO_CODE,
  CMA_CODES,
  INDICATOR_IDS,
  FREQUENCIES,
  DATA_SOURCES,
  TIERS,
  GEOGRAPHY_LEVELS,
} from './constants';
