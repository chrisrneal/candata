'use client';

import { useCallback } from 'react';
import useSWR from 'swr';
import { useAuth } from '@/hooks/use-auth';
import type {
  SavedReport,
  ReportDefinition,
} from '@/hooks/use-report-builder';

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const BASE_URL = process.env.NEXT_PUBLIC_API_URL ?? '/api/v1';

function apiUrl(path: string): string {
  if (BASE_URL.startsWith('http')) return `${BASE_URL}${path}`;
  const origin =
    typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3000';
  return `${origin}${BASE_URL}${path}`;
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export function useReports() {
  const { session } = useAuth();

  const authHeaders = useCallback(
    (extra?: HeadersInit): HeadersInit => {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        ...(extra as Record<string, string>),
      };
      if (session?.access_token) {
        headers['Authorization'] = `Bearer ${session.access_token}`;
      }
      return headers;
    },
    [session],
  );

  // ---- SWR fetcher ----
  const { data, error, isLoading, mutate } = useSWR<SavedReport[]>(
    session ? 'reports-list' : null,
    async () => {
      const res = await fetch(apiUrl('/reports'), {
        headers: authHeaders(),
      });
      if (!res.ok) throw new Error(`Failed to load reports (${res.status})`);
      const body = await res.json();
      return body.data ?? [];
    },
    {
      revalidateOnFocus: true,
      dedupingInterval: 5_000,
    },
  );

  // ---- Create ----
  const createReport = useCallback(
    async (
      definition: ReportDefinition,
      title: string,
      description?: string,
    ): Promise<SavedReport> => {
      const res = await fetch(apiUrl('/reports'), {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ ...definition, title, description: description ?? null }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.error?.message ?? `Create failed (${res.status})`);
      }
      const body = await res.json();
      const created = body.data as SavedReport;
      // Optimistic update
      await mutate((prev) => (prev ? [created, ...prev] : [created]), false);
      return created;
    },
    [authHeaders, mutate],
  );

  // ---- Update ----
  const updateReport = useCallback(
    async (
      id: string,
      partial: Partial<Pick<SavedReport, 'title' | 'description' | 'definition'>>,
    ): Promise<SavedReport> => {
      const res = await fetch(apiUrl(`/reports/${id}`), {
        method: 'PUT',
        headers: authHeaders(),
        body: JSON.stringify(partial),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.error?.message ?? `Update failed (${res.status})`);
      }
      const body = await res.json();
      const updated = body.data as SavedReport;
      // Optimistic update
      await mutate(
        (prev) => prev?.map((r) => (r.id === id ? updated : r)),
        false,
      );
      return updated;
    },
    [authHeaders, mutate],
  );

  // ---- Delete (soft) ----
  const deleteReport = useCallback(
    async (id: string): Promise<void> => {
      // Optimistic removal
      await mutate((prev) => prev?.filter((r) => r.id !== id), false);

      const res = await fetch(apiUrl(`/reports/${id}`), {
        method: 'DELETE',
        headers: authHeaders(),
      });
      if (!res.ok) {
        // Rollback optimistic update on failure
        await mutate();
        const body = await res.json().catch(() => null);
        throw new Error(body?.error?.message ?? `Delete failed (${res.status})`);
      }
    },
    [authHeaders, mutate],
  );

  // ---- Refetch ----
  const refetch = useCallback(() => mutate(), [mutate]);

  return {
    reports: data ?? [],
    isLoading,
    error: error as Error | undefined,
    createReport,
    updateReport,
    deleteReport,
    refetch,
  } as const;
}
