/** Types for the custom report builder. */

export interface SavedReport {
  id: string;
  user_id: string;
  title: string;
  description: string | null;
  definition: ReportDefinition;
  last_run_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface ReportDefinition {
  domain: 'indicators' | 'housing' | 'procurement' | 'trade';
  metric: string;
  filters: ReportFilters;
  visualization: 'line' | 'bar' | 'area' | 'table';
}

export interface ReportFilters {
  geo_codes: string[];
  date_from: string;
  date_to: string;
  group_by: 'province' | 'cma' | 'national';
  extra?: Record<string, unknown>;
}

export interface QueryResultColumn {
  key: string;
  label: string;
  type: 'string' | 'number' | 'date';
}

export interface QueryResultMeta {
  total_rows: number;
  query_ms: number;
}

export interface QueryResult {
  columns: QueryResultColumn[];
  rows: Record<string, unknown>[];
  meta: QueryResultMeta;
}
