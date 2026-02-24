'use client';

import { useQuery } from '@tanstack/react-query';
import { fetchHousingVacancy, fetchHousingRents, fetchHousingStarts } from '@/lib/api-client';

export function useVacancyRates(params?: Record<string, string | number | undefined>) {
  return useQuery({
    queryKey: ['vacancy-rates', params],
    queryFn: () => fetchHousingVacancy(params),
  });
}

export function useAverageRents(params?: Record<string, string | number | undefined>) {
  return useQuery({
    queryKey: ['average-rents', params],
    queryFn: () => fetchHousingRents(params),
  });
}

export function useHousingStarts(params?: Record<string, string | number | undefined>) {
  return useQuery({
    queryKey: ['housing-starts', params],
    queryFn: () => fetchHousingStarts(params),
  });
}
