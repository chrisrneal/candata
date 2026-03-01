'use client';

import { useState, useCallback, useEffect, useRef } from 'react';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { toast } from 'sonner';
import {
  ArrowLeft,
  Play,
  Save,
  Loader2,
  SlidersHorizontal,
  X,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ReportConfig } from '@/components/reports/ReportConfig';
import { ResultsPanel } from '@/components/reports/ResultsPanel';
import {
  useReportBuilder,
  type ReportDefinition,
} from '@/hooks/use-report-builder';

// ---------------------------------------------------------------------------
// URL param keys we sync
// ---------------------------------------------------------------------------

const URL_KEYS = ['domain', 'metric', 'date_from', 'date_to', 'group_by'] as const;

function definitionFromParams(
  params: URLSearchParams,
  fallback: ReportDefinition,
): ReportDefinition {
  const domain = (params.get('domain') as ReportDefinition['domain']) || fallback.domain;
  const metric = params.get('metric') || fallback.metric;
  const date_from = params.get('date_from') || fallback.filters.date_from;
  const date_to = params.get('date_to') || fallback.filters.date_to;
  const group_by = (params.get('group_by') as ReportDefinition['filters']['group_by']) || fallback.filters.group_by;

  return {
    ...fallback,
    domain,
    metric,
    filters: { ...fallback.filters, date_from, date_to, group_by },
  };
}

function paramsFromDefinition(def: ReportDefinition): Record<string, string> {
  const entries: Record<string, string> = {};
  if (def.domain) entries.domain = def.domain;
  if (def.metric) entries.metric = def.metric;
  if (def.filters.date_from) entries.date_from = def.filters.date_from;
  if (def.filters.date_to) entries.date_to = def.filters.date_to;
  if (def.filters.group_by) entries.group_by = def.filters.group_by;
  return entries;
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function NewReportPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const initialised = useRef(false);

  const {
    definition,
    setDefinition,
    results,
    isLoading,
    error,
    runQuery,
    saveReport,
  } = useReportBuilder();

  const [saving, setSaving] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);

  // ---- Initialise definition from URL on first mount ----
  useEffect(() => {
    if (initialised.current) return;
    initialised.current = true;

    const hasParams = URL_KEYS.some((k) => searchParams.has(k));
    if (hasParams) {
      setDefinition((prev) => definitionFromParams(searchParams, prev));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ---- Sync definition → URL (shallow, no navigation) ----
  useEffect(() => {
    if (!initialised.current) return;
    const next = paramsFromDefinition(definition);
    const sp = new URLSearchParams(next);
    const url = `${window.location.pathname}?${sp.toString()}`;
    window.history.replaceState(null, '', url);
  }, [definition]);

  // ---- Toast on query error ----
  useEffect(() => {
    if (error && error !== 'RATE_LIMIT') {
      toast.error(`Query failed: ${error}`);
    }
  }, [error]);

  const handleDefinitionChange = useCallback(
    (next: ReportDefinition) => {
      setDefinition(next);
      // Close drawer on mobile after a config change
      setDrawerOpen(false);
    },
    [setDefinition],
  );

  const handleSave = useCallback(async () => {
    if (!definition.title?.trim()) {
      document.querySelector<HTMLInputElement>('[data-title-input]')?.focus();
      return;
    }
    setSaving(true);
    const saved = await saveReport();
    setSaving(false);
    if (saved) {
      toast.success('Report saved');
      router.push(`/dashboard/reports/${saved.id}`);
    }
  }, [definition.title, saveReport, router]);

  return (
    <div className="flex flex-col h-[calc(100vh-5rem)]">
      {/* Top bar */}
      <div className="flex items-center gap-3 pb-4 border-b border-slate-800 mb-4">
        <Button asChild variant="ghost" size="icon" className="shrink-0">
          <Link href="/dashboard/reports">
            <ArrowLeft className="h-4 w-4" />
          </Link>
        </Button>

        <Input
          data-title-input
          value={definition.title ?? ''}
          onChange={(e) => setDefinition({ ...definition, title: e.target.value })}
          placeholder="Untitled Report"
          className="text-lg font-semibold border-none bg-transparent shadow-none focus-visible:ring-0 px-0 h-auto text-slate-100 placeholder:text-slate-600"
        />

        <div className="flex items-center gap-2 ml-auto shrink-0">
          {/* Mobile: configure button */}
          <Button
            variant="outline"
            size="icon"
            className="md:hidden shrink-0"
            onClick={() => setDrawerOpen(true)}
          >
            <SlidersHorizontal className="h-4 w-4" />
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={runQuery}
            disabled={isLoading || !definition.metric}
          >
            {isLoading ? (
              <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
            ) : (
              <Play className="h-3.5 w-3.5 mr-1.5" />
            )}
            Run
          </Button>
          <Button size="sm" onClick={handleSave} disabled={saving}>
            {saving ? (
              <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
            ) : (
              <Save className="h-3.5 w-3.5 mr-1.5" />
            )}
            Save
          </Button>
        </div>
      </div>

      {/* Two-panel layout */}
      <div className="flex flex-1 gap-4 min-h-0">
        {/* Left: config sidebar (hidden on mobile) */}
        <aside className="hidden md:block w-[300px] shrink-0 overflow-y-auto rounded-xl border border-slate-800 bg-slate-900 p-4">
          <ReportConfig definition={definition} onChange={handleDefinitionChange} />
        </aside>

        {/* Right: results (full width on mobile) */}
        <div className="flex-1 min-w-0 overflow-y-auto rounded-xl border border-slate-800 bg-slate-900 p-4 md:p-6">
          <ResultsPanel
            results={results}
            visualization={definition.visualization}
            isLoading={isLoading}
            error={error}
          />
        </div>
      </div>

      {/* Mobile: bottom drawer */}
      {drawerOpen && (
        <div className="fixed inset-0 z-50 md:hidden">
          {/* Backdrop */}
          <div
            className="absolute inset-0 bg-black/60"
            onClick={() => setDrawerOpen(false)}
          />
          {/* Drawer */}
          <div className="absolute bottom-0 left-0 right-0 max-h-[85vh] rounded-t-2xl border-t border-slate-700 bg-slate-900 overflow-y-auto animate-in slide-in-from-bottom duration-300">
            <div className="sticky top-0 flex items-center justify-between px-4 py-3 border-b border-slate-800 bg-slate-900 z-10">
              <span className="text-sm font-semibold text-slate-200">Configure Report</span>
              <button
                onClick={() => setDrawerOpen(false)}
                className="rounded-md p-1 text-slate-400 hover:text-slate-200 hover:bg-slate-800"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <div className="p-4">
              <ReportConfig definition={definition} onChange={handleDefinitionChange} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
