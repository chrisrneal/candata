/**
 * types/geography.ts — Geography interfaces matching the geographies table.
 */

export type GeographyLevel =
  | 'country'
  | 'pr'
  | 'cd'
  | 'csd'
  | 'cma'
  | 'ca'
  | 'fsa';

export interface Geography {
  id: string;
  level: GeographyLevel;
  sgc_code: string;
  name: string;
  name_fr: string | null;
  parent_id: string | null;
  properties: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface Province extends Geography {
  level: 'pr';
}

export interface CMA extends Geography {
  level: 'cma';
}

export interface CensusDivision extends Geography {
  level: 'cd';
}

export interface FSA extends Geography {
  level: 'fsa';
}
