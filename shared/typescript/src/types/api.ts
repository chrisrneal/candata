/**
 * types/api.ts — Generic API envelope types used by packages/api responses.
 */

export interface PaginationMeta {
  total: number;
  page: number;
  per_page: number;
  total_pages: number;
  has_next: boolean;
  has_prev: boolean;
}

export interface Links {
  self: string;
  next: string | null;
  prev: string | null;
  first: string;
  last: string;
}

export interface ApiResponse<T> {
  data: T[];
  meta: PaginationMeta;
  links: Links;
}

export interface ApiSingleResponse<T> {
  data: T;
}

export interface ApiError {
  error: {
    code: string;
    message: string;
    details?: Record<string, unknown>;
    docs_url?: string;
  };
}

/** Tier names, matching the database CHECK constraint */
export type Tier = 'free' | 'starter' | 'pro' | 'business' | 'enterprise';

/** Data source names */
export type DataSource = 'StatCan' | 'BoC' | 'CMHC' | 'CanadaBuys';

/** Time-series frequency */
export type Frequency =
  | 'daily'
  | 'weekly'
  | 'monthly'
  | 'quarterly'
  | 'semi-annual'
  | 'annual';
