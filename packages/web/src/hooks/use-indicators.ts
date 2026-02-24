'use client';

import { useQuery } from '@tanstack/react-query';
import { fetchIndicators, fetchIndicatorValues } from '@/lib/api-client';

export function useIndicators(params?: Record<string, string | number | undefined>) {
  return useQuery({
    queryKey: ['indicators', params],
    queryFn: () => fetchIndicators(params),
  });
}

export function useIndicatorValues(id: string, params?: Record<string, string | number | undefined>) {
  return useQuery({
    queryKey: ['indicator-values', id, params],
    queryFn: () => fetchIndicatorValues(id, params),
    enabled: !!id,
  });
}
