// Re-export all shared types
export type {
  ApiResponse,
  ApiError,
  PaginationMeta,
  Links,
  Indicator,
  IndicatorValue,
  VacancyRate,
  AverageRent,
  HousingStart,
  Contract,
  Tender,
  TradeFlow,
  Geography,
  Province,
  CMA,
  CensusDivision,
  FSA,
  GeographyLevel,
  Entity,
  EntityType,
  EntityRelationship,
} from '@candata/shared';

// Web-specific types

export interface NavItem {
  title: string;
  href: string;
  icon?: string;
  disabled?: boolean;
  children?: NavItem[];
}

export interface DashboardConfig {
  mainNav: NavItem[];
  sidebarNav: NavItem[];
}
