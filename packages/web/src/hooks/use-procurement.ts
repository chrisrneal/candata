'use client';

import { useQuery } from '@tanstack/react-query';
import { fetchContracts, fetchTenders } from '@/lib/api-client';

export function useContracts(params?: Record<string, string | number | undefined>) {
  return useQuery({
    queryKey: ['contracts', params],
    queryFn: () => fetchContracts(params),
  });
}

export function useTenders(params?: Record<string, string | number | undefined>) {
  return useQuery({
    queryKey: ['tenders', params],
    queryFn: () => fetchTenders(params),
  });
}
