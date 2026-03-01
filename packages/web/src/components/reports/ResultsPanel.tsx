'use client';

import { useState, useMemo, useCallback } from 'react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import {
  Play,
  AlertCircle,
  ChevronLeft,
  ChevronRight,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Download,
  Copy,
  Check,
  SearchX,
} from 'lucide-react';
import Link from 'next/link';
import { toast } from 'sonner';
import { Skeleton } from '@/components/ui/skeleton';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { QueryResult, QueryResultColumn } from '@/hooks/use-report-builder';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIES_COLORS = ['#34d399', '#cbd5e1', '#22d3ee', '#fbbf24', '#a78bfa', '#fb7185'];
const ROWS_PER_PAGE = 25;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Format large numbers with SI suffixes: 1.2M, 450K, etc. */
function formatValue(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  if (Number.isInteger(value)) return value.toLocaleString('en-CA');
  return value.toFixed(2);
}

/** Format YYYY-MM → "MMM 'YY" (e.g. "Jan '24") */
function formatDateLabel(raw: unknown): string {
  if (typeof raw !== 'string') return String(raw ?? '');
  // Handle YYYY-MM or YYYY-MM-DD
  const match = raw.match(/^(\d{4})-(\d{2})/);
  if (!match) return raw;
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const month = months[parseInt(match[2], 10) - 1] ?? match[2];
  const year = match[1].slice(2);
  return `${month} '${year}`;
}

/** Extract the date range string from result rows */
function getDateRange(rows: Record<string, unknown>[], dateKey: string | undefined): string {
  if (!dateKey || rows.length === 0) return '';
  const dates = rows.map((r) => String(r[dateKey] ?? '')).filter(Boolean).sort();
  if (dates.length === 0) return '';
  const first = formatDateLabel(dates[0]);
  const last = formatDateLabel(dates[dates.length - 1]);
  return first === last ? first : `${first} – ${last}`;
}

/** Generate a CSV string from results */
function toCsv(columns: QueryResultColumn[], rows: Record<string, unknown>[]): string {
  const escape = (v: unknown): string => {
    const s = v == null ? '' : String(v);
    return s.includes(',') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  };
  const header = columns.map((c) => escape(c.label)).join(',');
  const body = rows.map((row) => columns.map((c) => escape(row[c.key])).join(',')).join('\n');
  return `${header}\n${body}`;
}

/** Trigger a browser file download */
function downloadFile(content: string, filename: string, mime = 'text/csv') {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// Custom Tooltip
// ---------------------------------------------------------------------------

function CustomTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{ color: string; name: string; value: number }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 shadow-xl text-xs">
      <p className="text-slate-400 mb-1.5 font-medium">{formatDateLabel(label)}</p>
      {payload.map((entry) => (
        <div key={entry.name} className="flex items-center gap-2">
          <span
            className="h-2 w-2 rounded-full shrink-0"
            style={{ backgroundColor: entry.color }}
          />
          <span className="text-slate-300">{entry.name}</span>
          <span className="ml-auto tabular-nums text-slate-100 font-medium">
            {formatValue(entry.value)}
          </span>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Summary Bar
// ---------------------------------------------------------------------------

function SummaryBar({
  results,
  dateKey,
  onExportCsv,
}: {
  results: QueryResult;
  dateKey: string | undefined;
  onExportCsv: () => void;
}) {
  const [copied, setCopied] = useState(false);

  const dateRange = useMemo(
    () => getDateRange(results.rows, dateKey),
    [results.rows, dateKey],
  );

  const handleCopyUrl = useCallback(() => {
    const base = process.env.NEXT_PUBLIC_API_URL ?? '/api/v1';
    const origin = typeof window !== 'undefined' ? window.location.origin : '';
    const url = `${origin}${base.startsWith('http') ? '' : base}/reports/query`;
    navigator.clipboard.writeText(url).then(() => {
      setCopied(true);
      toast('Copied API URL to clipboard');
      setTimeout(() => setCopied(false), 2000);
    });
  }, []);

  return (
    <div className="flex items-center gap-3 rounded-lg border border-slate-800 bg-slate-800/40 px-4 py-2.5 text-xs">
      <span className="text-slate-400">
        <strong className="text-slate-200 tabular-nums">
          {results.meta.total_rows.toLocaleString('en-CA')}
        </strong>{' '}
        rows
      </span>

      {dateRange && (
        <>
          <span className="text-slate-700">|</span>
          <span className="text-slate-400">{dateRange}</span>
        </>
      )}

      <span className="text-slate-700">|</span>
      <span className="text-slate-500">{results.meta.query_ms}ms</span>

      <div className="ml-auto flex items-center gap-1.5">
        <Button
          variant="ghost"
          size="sm"
          className="h-6 px-2 text-xs text-slate-400 hover:text-slate-200"
          onClick={handleCopyUrl}
        >
          {copied ? (
            <Check className="h-3 w-3 mr-1 text-emerald-400" />
          ) : (
            <Copy className="h-3 w-3 mr-1" />
          )}
          {copied ? 'Copied' : 'Copy API URL'}
        </Button>
        <Button
          variant="ghost"
          size="sm"
          className="h-6 px-2 text-xs text-slate-400 hover:text-slate-200"
          onClick={onExportCsv}
        >
          <Download className="h-3 w-3 mr-1" />
          CSV
        </Button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Chart View
// ---------------------------------------------------------------------------

function ChartView({
  results,
  visualization,
}: {
  results: QueryResult;
  visualization: 'line' | 'bar' | 'area';
}) {
  const dateCol = results.columns.find((c) => c.type === 'date');
  const stringCol = results.columns.find((c) => c.type === 'string');
  const numberCols = results.columns.filter((c) => c.type === 'number');
  const xKey = dateCol?.key ?? stringCol?.key ?? results.columns[0]?.key ?? 'date';
  const isDateAxis = !!dateCol;

  if (numberCols.length === 0) {
    return (
      <div className="flex items-center justify-center h-[380px] text-slate-500 text-sm">
        No numeric columns to chart.
      </div>
    );
  }

  const chartMargin = { top: 10, right: 16, left: 8, bottom: 5 };

  const commonAxisProps = {
    stroke: '#475569',
    fontSize: 11,
    tickLine: false as const,
    axisLine: false as const,
  };

  const xAxisProps = {
    ...commonAxisProps,
    dataKey: xKey,
    ...(isDateAxis ? { tickFormatter: (v: string) => formatDateLabel(v) } : {}),
  };

  const yAxisProps = {
    ...commonAxisProps,
    tickFormatter: (v: number) => formatValue(v),
    width: 56,
  };

  if (visualization === 'bar') {
    return (
      <ResponsiveContainer width="100%" height={380}>
        <BarChart data={results.rows} margin={chartMargin}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
          <XAxis {...xAxisProps} />
          <YAxis {...yAxisProps} />
          <RechartsTooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(148,163,184,0.08)' }} />
          <Legend
            wrapperStyle={{ fontSize: 11, color: '#94a3b8', paddingTop: 8 }}
          />
          {numberCols.map((col, i) => (
            <Bar
              key={col.key}
              dataKey={col.key}
              name={col.label}
              fill={SERIES_COLORS[i % SERIES_COLORS.length]}
              radius={[3, 3, 0, 0]}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    );
  }

  if (visualization === 'area') {
    return (
      <ResponsiveContainer width="100%" height={380}>
        <AreaChart data={results.rows} margin={chartMargin}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
          <XAxis {...xAxisProps} />
          <YAxis {...yAxisProps} />
          <RechartsTooltip content={<CustomTooltip />} />
          <Legend
            wrapperStyle={{ fontSize: 11, color: '#94a3b8', paddingTop: 8 }}
          />
          {numberCols.map((col, i) => {
            const color = SERIES_COLORS[i % SERIES_COLORS.length];
            return (
              <Area
                key={col.key}
                type="monotone"
                dataKey={col.key}
                name={col.label}
                stroke={color}
                fill={color}
                fillOpacity={0.12}
                strokeWidth={2}
              />
            );
          })}
        </AreaChart>
      </ResponsiveContainer>
    );
  }

  // Default: line
  return (
    <ResponsiveContainer width="100%" height={380}>
      <LineChart data={results.rows} margin={chartMargin}>
        <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
        <XAxis {...xAxisProps} />
        <YAxis {...yAxisProps} />
        <RechartsTooltip content={<CustomTooltip />} />
        <Legend
          wrapperStyle={{ fontSize: 11, color: '#94a3b8', paddingTop: 8 }}
        />
        {numberCols.map((col, i) => {
          const color = SERIES_COLORS[i % SERIES_COLORS.length];
          return (
            <Line
              key={col.key}
              type="monotone"
              dataKey={col.key}
              name={col.label}
              stroke={color}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: color, stroke: '#0f172a', strokeWidth: 2 }}
            />
          );
        })}
      </LineChart>
    </ResponsiveContainer>
  );
}

// ---------------------------------------------------------------------------
// Table View
// ---------------------------------------------------------------------------

type SortDir = 'asc' | 'desc';

function TableView({ results }: { results: QueryResult }) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>('asc');
  const [page, setPage] = useState(0);

  const toggleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
    setPage(0);
  };

  const sortedRows = useMemo(() => {
    if (!sortKey) return results.rows;
    return [...results.rows].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      const cmp = typeof av === 'number' && typeof bv === 'number'
        ? av - bv
        : String(av).localeCompare(String(bv));
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }, [results.rows, sortKey, sortDir]);

  const totalPages = Math.max(1, Math.ceil(sortedRows.length / ROWS_PER_PAGE));
  const pageRows = sortedRows.slice(page * ROWS_PER_PAGE, (page + 1) * ROWS_PER_PAGE);

  const SortIcon = ({ colKey }: { colKey: string }) => {
    if (sortKey !== colKey) return <ArrowUpDown className="h-3 w-3 opacity-0 group-hover:opacity-50" />;
    return sortDir === 'asc'
      ? <ArrowUp className="h-3 w-3 text-brand-400" />
      : <ArrowDown className="h-3 w-3 text-brand-400" />;
  };

  return (
    <div className="space-y-3">
      <div className="rounded-lg border border-slate-800 overflow-auto max-h-[480px]">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-slate-900 sticky top-0 z-10">
              {results.columns.map((col) => (
                <th
                  key={col.key}
                  onClick={() => toggleSort(col.key)}
                  className={cn(
                    'group cursor-pointer select-none border-b border-slate-800 px-3 py-2.5 text-xs font-medium text-slate-400 whitespace-nowrap transition-colors hover:text-slate-200',
                    col.type === 'number' ? 'text-right' : 'text-left',
                  )}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.label}
                    <SortIcon colKey={col.key} />
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageRows.map((row, i) => (
              <tr
                key={i}
                className={cn(
                  'border-b border-slate-800/40 transition-colors hover:bg-slate-800/60',
                  i % 2 === 0 ? 'bg-slate-900' : 'bg-slate-800/30',
                )}
              >
                {results.columns.map((col) => (
                  <td
                    key={col.key}
                    className={cn(
                      'px-3 py-2 text-slate-300 whitespace-nowrap',
                      col.type === 'number' ? 'text-right tabular-nums' : 'text-left',
                    )}
                  >
                    {row[col.key] != null
                      ? col.type === 'number'
                        ? formatValue(row[col.key] as number)
                        : col.type === 'date'
                          ? formatDateLabel(row[col.key])
                          : String(row[col.key])
                      : '—'}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between text-xs text-slate-400">
          <span>
            Page {page + 1} of {totalPages}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-7 p-0"
              disabled={page === 0}
              onClick={() => setPage((p) => p - 1)}
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-7 p-0"
              disabled={page >= totalPages - 1}
              onClick={() => setPage((p) => p + 1)}
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// ResultsPanel (main export)
// ---------------------------------------------------------------------------

export interface ResultsPanelProps {
  results: QueryResult | null;
  visualization: 'line' | 'bar' | 'area' | 'table';
  isLoading: boolean;
  error: string | null;
}

export function ResultsPanel({
  results,
  visualization,
  isLoading,
  error,
}: ResultsPanelProps) {
  const [view, setView] = useState<'chart' | 'table'>(
    visualization === 'table' ? 'table' : 'chart',
  );

  // Derive the date column key for the summary bar
  const dateKey = results?.columns.find((c) => c.type === 'date')?.key;

  const handleExportCsv = useCallback(() => {
    if (!results) return;
    const csv = toCsv(results.columns, results.rows);
    downloadFile(csv, `report-${Date.now()}.csv`);
  }, [results]);

  // ---- Loading state ----
  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-10 w-full rounded-lg" />
        {/* ChartSkeleton – animated pulse bars */}
        <div className="h-[380px] w-full rounded-lg border border-slate-800 bg-slate-900 p-6 flex items-end gap-3">
          {[65, 45, 80, 55, 70, 40, 90, 50, 75, 60, 85, 48].map((h, i) => (
            <div
              key={i}
              className="flex-1 animate-pulse rounded-t bg-slate-800"
              style={{ height: `${h}%`, animationDelay: `${i * 75}ms` }}
            />
          ))}
        </div>
      </div>
    );
  }

  // ---- Error state ----
  if (error) {
    const isRateLimit = error === 'RATE_LIMIT';
    return (
      <div className="flex flex-col items-center justify-center h-full py-16">
        <div className={cn(
          'rounded-lg border px-6 py-5 max-w-md text-center',
          isRateLimit
            ? 'border-amber-900/50 bg-amber-950/20'
            : 'border-red-900/50 bg-red-950/20',
        )}>
          <AlertCircle className={cn('h-6 w-6 mx-auto mb-2', isRateLimit ? 'text-amber-400' : 'text-red-400')} />
          {isRateLimit ? (
            <>
              <p className="text-sm text-amber-400 leading-relaxed mb-3">
                Query limit reached. Upgrade your plan for more queries.
              </p>
              <Link
                href="/billing"
                className="inline-flex items-center text-xs font-medium text-brand-400 hover:text-brand-300"
              >
                View plans &rarr;
              </Link>
            </>
          ) : (
            <p className="text-sm text-red-400 leading-relaxed">{error}</p>
          )}
        </div>
      </div>
    );
  }

  // ---- Empty state ----
  if (!results) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-center py-16">
        <div className="rounded-full bg-slate-800 p-4">
          <Play className="h-7 w-7 text-slate-500" />
        </div>
        <div>
          <p className="text-sm text-slate-400 font-medium">Run your query to see results</p>
          <p className="text-xs text-slate-500 mt-1">
            Configure your report on the left, then click{' '}
            <strong className="text-slate-400">Run</strong>.
          </p>
        </div>
      </div>
    );
  }

  // ---- Zero-row result ----
  if (results.rows.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-center py-16">
        <div className="rounded-full bg-slate-800 p-4">
          <SearchX className="h-7 w-7 text-slate-500" />
        </div>
        <div>
          <p className="text-sm text-slate-400 font-medium">No data found for your selection</p>
          <p className="text-xs text-slate-500 mt-1 max-w-xs mx-auto">
            Try adjusting the date range or geography filters to broaden your query.
          </p>
        </div>
      </div>
    );
  }

  // ---- Results present ----
  return (
    <div className="space-y-4">
      {/* Summary bar */}
      <SummaryBar results={results} dateKey={dateKey} onExportCsv={handleExportCsv} />

      {/* View toggle (only when not table-only visualization) */}
      {visualization !== 'table' && (
        <div className="flex items-center gap-1 rounded-md border border-slate-700 p-0.5 w-fit">
          <button
            onClick={() => setView('chart')}
            className={cn(
              'rounded px-3 py-1 text-xs font-medium transition-colors',
              view === 'chart'
                ? 'bg-slate-700 text-slate-100'
                : 'text-slate-400 hover:text-slate-200',
            )}
          >
            Chart
          </button>
          <button
            onClick={() => setView('table')}
            className={cn(
              'rounded px-3 py-1 text-xs font-medium transition-colors',
              view === 'table'
                ? 'bg-slate-700 text-slate-100'
                : 'text-slate-400 hover:text-slate-200',
            )}
          >
            Table
          </button>
        </div>
      )}

      {/* Content */}
      {view === 'chart' && visualization !== 'table' ? (
        <ChartView results={results} visualization={visualization} />
      ) : (
        <TableView results={results} />
      )}
    </div>
  );
}
