/**
 * types/housing.ts — CMHC housing market data interfaces.
 */

export type BedroomType = 'bachelor' | '1br' | '2br' | '3br+' | 'total';
export type DwellingType = 'single' | 'semi' | 'row' | 'apartment' | 'total';

export interface VacancyRate {
  id: string;
  geography_id: string;
  ref_date: string;            // ISO date
  bedroom_type: BedroomType;
  vacancy_rate: number | null; // percent, e.g. 2.5
  universe: number | null;
  created_at: string;
  updated_at: string;
}

export interface AverageRent {
  id: string;
  geography_id: string;
  ref_date: string;
  bedroom_type: BedroomType;
  average_rent: number | null; // CAD per month
  created_at: string;
  updated_at: string;
}

export interface HousingStart {
  id: string;
  geography_id: string;
  ref_date: string;
  dwelling_type: DwellingType;
  units: number | null;
  created_at: string;
  updated_at: string;
}
