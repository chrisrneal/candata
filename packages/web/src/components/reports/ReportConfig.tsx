'use client';

import { useState } from 'react';
import { ChevronDown, LineChart, BarChart3, AreaChart, Table2 } from 'lucide-react';
import { PROVINCES, CMA_CODES } from '@candata/shared';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { cn } from '@/lib/utils';
import type { ReportDefinition } from '@/hooks/use-report-builder';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DOMAINS = [
  { value: 'indicators', label: 'Economic Indicators' },
  { value: 'housing', label: 'Housing' },
  { value: 'procurement', label: 'Procurement' },
  { value: 'trade', label: 'Trade' },
] as const;

const METRICS: Record<string, { value: string; label: string }[]> = {
  indicators: [
    { value: 'cpi', label: 'CPI' },
    { value: 'gdp', label: 'GDP' },
    { value: 'unemployment_rate', label: 'Unemployment Rate' },
    { value: 'employment', label: 'Employment' },
    { value: 'retail_sales', label: 'Retail Sales' },
    { value: 'overnight_rate', label: 'Overnight Rate' },
    { value: 'prime_rate', label: 'Prime Rate' },
    { value: 'usdcad', label: 'USD/CAD' },
  ],
  housing: [
    { value: 'vacancy_rate', label: 'Vacancy Rate' },
    { value: 'average_rent', label: 'Average Rent' },
    { value: 'housing_starts', label: 'Housing Starts' },
    { value: 'building_permits', label: 'Building Permits' },
  ],
  procurement: [
    { value: 'contract_value', label: 'Contract Value' },
    { value: 'contract_count', label: 'Contract Count' },
    { value: 'tender_value', label: 'Tender Value' },
  ],
  trade: [
    { value: 'export_value', label: 'Export Value' },
    { value: 'import_value', label: 'Import Value' },
    { value: 'trade_balance', label: 'Trade Balance' },
  ],
};

const VIZ_OPTIONS = [
  { value: 'line', label: 'Line', icon: LineChart },
  { value: 'bar', label: 'Bar', icon: BarChart3 },
  { value: 'area', label: 'Area', icon: AreaChart },
  { value: 'table', label: 'Table', icon: Table2 },
] as const;

/** Top 15 CMAs – subset of CMA_CODES keyed by code. */
const TOP_CMA_CODES = [
  '001', '002', '003', '004', '005', '006', '007', '008', '009',
  '010', '014', '012', '015', '018', '019',
] as const;

const TOP_CMAS = TOP_CMA_CODES.map((code) => ({
  code,
  name: CMA_CODES[code] ?? code,
}));

// Date helper – compute YYYY-MM relative to today
function monthsAgo(n: number): string {
  const d = new Date();
  d.setMonth(d.getMonth() - n);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}

const DATE_PRESETS = [
  { label: 'Last 12 months', from: () => monthsAgo(12) },
  { label: 'Last 3 years', from: () => monthsAgo(36) },
  { label: 'Last 5 years', from: () => monthsAgo(60) },
  { label: 'All time', from: () => '' },
] as const;

// ---------------------------------------------------------------------------
// Collapsible section wrapper
// ---------------------------------------------------------------------------

function Section({
  title,
  children,
  defaultOpen = true,
}: {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center justify-between py-2 text-xs font-semibold uppercase tracking-wider text-slate-400 hover:text-slate-300 transition-colors"
      >
        {title}
        <ChevronDown
          className={cn(
            'h-3.5 w-3.5 transition-transform',
            open ? 'rotate-0' : '-rotate-90',
          )}
        />
      </button>
      {open && <div className="space-y-3 pb-4">{children}</div>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// ReportConfig
// ---------------------------------------------------------------------------

export interface ReportConfigProps {
  definition: ReportDefinition;
  onChange: (definition: ReportDefinition) => void;
}

export function ReportConfig({ definition, onChange }: ReportConfigProps) {
  const metrics = METRICS[definition.domain] ?? [];
  const provinces = Object.entries(PROVINCES);

  // Partial updaters that always call onChange with a full ReportDefinition
  const set = (patch: Partial<ReportDefinition>) =>
    onChange({ ...definition, ...patch, filters: patch.filters ?? definition.filters });

  const setFilters = (patch: Partial<ReportDefinition['filters']>) =>
    onChange({ ...definition, filters: { ...definition.filters, ...patch } });

  const toggleGeoCode = (code: string) => {
    const codes = definition.filters.geo_codes;
    const next = codes.includes(code)
      ? codes.filter((c) => c !== code)
      : [...codes, code];
    setFilters({ geo_codes: next });
  };

  return (
    <div className="divide-y divide-slate-800">
      {/* =============== DATA SOURCE =============== */}
      <Section title="Data Source">
        {/* Domain – segmented control */}
        <div className="space-y-1.5">
          <label className="text-xs text-slate-500">Domain</label>
          <div className="grid grid-cols-2 gap-1 rounded-lg border border-slate-700 bg-slate-800/50 p-1">
            {DOMAINS.map((d) => (
              <button
                key={d.value}
                type="button"
                onClick={() =>
                  set({ domain: d.value as ReportDefinition['domain'], metric: '' })
                }
                className={cn(
                  'rounded-md px-2 py-1.5 text-xs font-medium transition-colors text-center',
                  definition.domain === d.value
                    ? 'bg-brand-600 text-white shadow-sm'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50',
                )}
              >
                {d.label}
              </button>
            ))}
          </div>
        </div>

        {/* Metric */}
        <div className="space-y-1.5">
          <label className="text-xs text-slate-500">Metric</label>
          <Select
            value={definition.metric}
            onValueChange={(v) => set({ metric: v })}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select metric…" />
            </SelectTrigger>
            <SelectContent>
              {metrics.map((m) => (
                <SelectItem key={m.value} value={m.value}>
                  {m.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </Section>

      {/* =============== GEOGRAPHY =============== */}
      <Section title="Geography">
        {/* Group by – radio buttons */}
        <div className="space-y-1.5">
          <label className="text-xs text-slate-500">Group By</label>
          <div className="flex gap-1 rounded-lg border border-slate-700 bg-slate-800/50 p-1">
            {(['national', 'province', 'cma'] as const).map((g) => (
              <button
                key={g}
                type="button"
                onClick={() => setFilters({ group_by: g, geo_codes: [] })}
                className={cn(
                  'flex-1 rounded-md px-2 py-1.5 text-xs font-medium capitalize transition-colors text-center',
                  definition.filters.group_by === g
                    ? 'bg-brand-600 text-white shadow-sm'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50',
                )}
              >
                {g === 'cma' ? 'CMA' : g.charAt(0).toUpperCase() + g.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {/* Province multi-select */}
        {definition.filters.group_by === 'province' && (
          <div className="space-y-1.5">
            <label className="text-xs text-slate-500">
              Provinces{' '}
              <span className="text-slate-600">
                ({definition.filters.geo_codes.length || 'all'})
              </span>
            </label>
            <div className="max-h-48 overflow-y-auto rounded-md border border-slate-700 bg-slate-800/50 divide-y divide-slate-800">
              {provinces.map(([code, name]) => (
                <label
                  key={code}
                  className="flex items-center gap-2 px-2.5 py-1.5 text-xs cursor-pointer hover:bg-slate-700/40 transition-colors"
                >
                  <input
                    type="checkbox"
                    checked={definition.filters.geo_codes.includes(code)}
                    onChange={() => toggleGeoCode(code)}
                    className="h-3.5 w-3.5 rounded border-slate-600 bg-slate-800 text-brand-500 focus:ring-brand-500 focus:ring-offset-0"
                  />
                  <span className="text-slate-300">{name}</span>
                  <span className="ml-auto text-slate-600">{code}</span>
                </label>
              ))}
            </div>
          </div>
        )}

        {/* CMA multi-select */}
        {definition.filters.group_by === 'cma' && (
          <div className="space-y-1.5">
            <label className="text-xs text-slate-500">
              CMAs{' '}
              <span className="text-slate-600">
                ({definition.filters.geo_codes.length || 'all'})
              </span>
            </label>
            <div className="max-h-48 overflow-y-auto rounded-md border border-slate-700 bg-slate-800/50 divide-y divide-slate-800">
              {TOP_CMAS.map((cma) => (
                <label
                  key={cma.code}
                  className="flex items-center gap-2 px-2.5 py-1.5 text-xs cursor-pointer hover:bg-slate-700/40 transition-colors"
                >
                  <input
                    type="checkbox"
                    checked={definition.filters.geo_codes.includes(cma.code)}
                    onChange={() => toggleGeoCode(cma.code)}
                    className="h-3.5 w-3.5 rounded border-slate-600 bg-slate-800 text-brand-500 focus:ring-brand-500 focus:ring-offset-0"
                  />
                  <span className="text-slate-300">{cma.name}</span>
                </label>
              ))}
            </div>
          </div>
        )}
      </Section>

      {/* =============== DATE RANGE =============== */}
      <Section title="Date Range">
        <div className="grid grid-cols-2 gap-2">
          <div className="space-y-1">
            <label className="text-xs text-slate-500">From</label>
            <Input
              type="month"
              value={definition.filters.date_from}
              onChange={(e) => setFilters({ date_from: e.target.value })}
              className="text-xs"
            />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-slate-500">To</label>
            <Input
              type="month"
              value={definition.filters.date_to}
              onChange={(e) => setFilters({ date_to: e.target.value })}
              className="text-xs"
            />
          </div>
        </div>

        {/* Quick presets */}
        <div className="flex flex-wrap gap-1.5">
          {DATE_PRESETS.map((preset) => {
            const presetFrom = preset.from();
            const isActive =
              definition.filters.date_from === presetFrom &&
              definition.filters.date_to === '';
            return (
              <button
                key={preset.label}
                type="button"
                onClick={() => setFilters({ date_from: presetFrom, date_to: '' })}
                className={cn(
                  'rounded-full px-2.5 py-1 text-xs font-medium transition-colors',
                  isActive
                    ? 'bg-brand-600/20 text-brand-400 border border-brand-600/30'
                    : 'text-slate-400 bg-slate-800 hover:bg-slate-700 border border-slate-700',
                )}
              >
                {preset.label}
              </button>
            );
          })}
        </div>
      </Section>

      {/* =============== VISUALIZATION =============== */}
      <Section title="Visualization">
        <div className="grid grid-cols-4 gap-1">
          {VIZ_OPTIONS.map((v) => (
            <button
              key={v.value}
              type="button"
              onClick={() =>
                set({ visualization: v.value as ReportDefinition['visualization'] })
              }
              className={cn(
                'flex flex-col items-center gap-1 rounded-md p-2.5 text-xs font-medium transition-colors',
                definition.visualization === v.value
                  ? 'bg-brand-600/20 text-brand-400 border border-brand-600/30'
                  : 'text-slate-400 hover:bg-slate-800 border border-transparent hover:text-slate-200',
              )}
            >
              <v.icon className="h-4 w-4" />
              {v.label}
            </button>
          ))}
        </div>
      </Section>
    </div>
  );
}
