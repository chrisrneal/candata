'use client';

import { useState, useCallback, useRef } from 'react';
import { useAuth } from '@/hooks/use-auth';

// ---------------------------------------------------------------------------
// Types (mirrors @candata/shared ReportDefinition / QueryResult)
// ---------------------------------------------------------------------------

export interface ReportFilters {
  geo_codes: string[];
  date_from: string;
  date_to: string;
  group_by: 'province' | 'cma' | 'national';
  extra?: Record<string, unknown>;
}

export interface ReportDefinition {
  title?: string;
  description?: string | null;
  domain: 'indicators' | 'housing' | 'procurement' | 'trade';
  metric: string;
  filters: ReportFilters;
  visualization: 'line' | 'bar' | 'area' | 'table';
}

export interface QueryResultColumn {
  key: string;
  label: string;
  type: 'string' | 'number' | 'date';
}

export interface QueryResult {
  columns: QueryResultColumn[];
  rows: Record<string, unknown>[];
  meta: { total_rows: number; query_ms: number };
}

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

// ---------------------------------------------------------------------------
// Default report definition
// ---------------------------------------------------------------------------

const DEFAULT_DEFINITION: ReportDefinition = {
  title: '',
  description: null,
  domain: 'indicators',
  metric: '',
  filters: {
    geo_codes: [],
    date_from: '',
    date_to: '',
    group_by: 'national',
  },
  visualization: 'line',
};

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

const BASE_URL = process.env.NEXT_PUBLIC_API_URL ?? '/api/v1';

function apiUrl(path: string): string {
  if (BASE_URL.startsWith('http')) return `${BASE_URL}${path}`;
  const origin =
    typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3000';
  return `${origin}${BASE_URL}${path}`;
}

export function useReportBuilder(initial?: Partial<ReportDefinition>) {
  const { session } = useAuth();

  const [definition, setDefinition] = useState<ReportDefinition>({
    ...DEFAULT_DEFINITION,
    ...initial,
    filters: { ...DEFAULT_DEFINITION.filters, ...initial?.filters },
  });
  const [results, setResults] = useState<QueryResult | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Abort controller ref for cancelling in-flight requests
  const abortRef = useRef<AbortController | null>(null);

  const authHeaders = useCallback((): HeadersInit => {
    const headers: HeadersInit = { 'Content-Type': 'application/json' };
    if (session?.access_token) {
      headers['Authorization'] = `Bearer ${session.access_token}`;
    }
    return headers;
  }, [session]);

  // ------ POST /v1/reports/query — ad-hoc query ------
  const runQuery = useCallback(async () => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setIsLoading(true);
    setError(null);

    try {
      const res = await fetch(apiUrl('/reports/query'), {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify(definition),
        signal: controller.signal,
      });

      if (!res.ok) {
        const body = await res.json().catch(() => null);
        if (res.status === 429) {
          throw new Error('RATE_LIMIT');
        }
        throw new Error(body?.error?.message ?? `Query failed (${res.status})`);
      }

      const data: QueryResult = await res.json();
      setResults(data);
      return data;
    } catch (err: unknown) {
      if (err instanceof DOMException && err.name === 'AbortError') return;
      const message = err instanceof Error ? err.message : 'Unknown error';
      setError(message);
      return undefined;
    } finally {
      setIsLoading(false);
    }
  }, [definition, authHeaders]);

  // ------ POST /v1/reports — save a new report ------
  const saveReport = useCallback(async (): Promise<SavedReport | undefined> => {
    setIsLoading(true);
    setError(null);

    try {
      const res = await fetch(apiUrl('/reports'), {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify(definition),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.error?.message ?? `Save failed (${res.status})`);
      }

      const data = await res.json();
      return data.data as SavedReport;
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Unknown error';
      setError(message);
      return undefined;
    } finally {
      setIsLoading(false);
    }
  }, [definition, authHeaders]);

  // ------ PUT /v1/reports/:id — update existing ------
  const updateReport = useCallback(
    async (id: string): Promise<SavedReport | undefined> => {
      setIsLoading(true);
      setError(null);

      try {
        const res = await fetch(apiUrl(`/reports/${id}`), {
          method: 'PUT',
          headers: authHeaders(),
          body: JSON.stringify(definition),
        });

        if (!res.ok) {
          const body = await res.json().catch(() => null);
          throw new Error(body?.error?.message ?? `Update failed (${res.status})`);
        }

        const data = await res.json();
        return data.data as SavedReport;
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : 'Unknown error';
        setError(message);
        return undefined;
      } finally {
        setIsLoading(false);
      }
    },
    [definition, authHeaders],
  );

  return {
    definition,
    setDefinition,
    results,
    isLoading,
    error,
    runQuery,
    saveReport,
    updateReport,
  } as const;
}
