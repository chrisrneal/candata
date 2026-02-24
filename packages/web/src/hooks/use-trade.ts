'use client';

import { useQuery } from '@tanstack/react-query';
import { fetchTrade } from '@/lib/api-client';

export function useTradeFlows(params?: Record<string, string | number | undefined>) {
  return useQuery({
    queryKey: ['trade-flows', params],
    queryFn: () => fetchTrade(params),
  });
}
