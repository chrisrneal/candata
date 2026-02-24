/**
 * types/indicators.ts — Indicator and IndicatorValue interfaces.
 */

import type { Frequency, DataSource } from './api.js';

export interface Indicator {
  id: string;
  name: string;
  source: DataSource;
  frequency: Frequency;
  unit: string;
  description: string | null;
  source_url: string | null;
  statcan_pid: string | null;
  boc_series: string | null;
  created_at: string;
  updated_at: string;
}

export interface IndicatorValue {
  indicator_id: string;
  geography_id: string;
  ref_date: string;          // ISO date string "YYYY-MM-DD"
  value: number | null;
  revision_date: string;
}

/** IndicatorValue with joined geography and indicator for API responses */
export interface IndicatorValueWithMeta extends IndicatorValue {
  indicator: Pick<Indicator, 'id' | 'name' | 'unit' | 'frequency'>;
  geography_name: string;
  geography_level: string;
  sgc_code: string;
}
