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
import { Skeleton } from '@/components/ui/skeleton';
import { ReportConfig } from '@/components/reports/ReportConfig';
import { ResultsPanel } from '@/components/reports/ResultsPanel';
import { useAuth } from '@/hooks/use-auth';
import {
  useReportBuilder,
  type ReportDefinition,
  type SavedReport,
} from '@/hooks/use-report-builder';

// ---------------------------------------------------------------------------
// Fetch a single saved report
// ---------------------------------------------------------------------------

const BASE_URL = process.env.NEXT_PUBLIC_API_URL ?? '/api/v1';

function apiUrl(path: string): string {
  if (BASE_URL.startsWith('http')) return `${BASE_URL}${path}`;
  const origin =
    typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3000';
  return `${origin}${BASE_URL}${path}`;
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function EditReportPage({ params }: { params: { id: string } }) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { session } = useAuth();

  const [saved, setSaved] = useState<SavedReport | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadingReport, setLoadingReport] = useState(true);

  const {
    definition,
    setDefinition,
    results,
    isLoading,
    error,
    runQuery,
    updateReport,
  } = useReportBuilder();

  const [saving, setSaving] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const hasAutoRun = useRef(false);

  // ---- Load the saved report ----
  useEffect(() => {
    if (!session?.access_token) return;

    let cancelled = false;

    (async () => {
      try {
        const res = await fetch(apiUrl(`/reports/${params.id}`), {
          headers: { Authorization: `Bearer ${session.access_token}` },
        });
        if (!res.ok) throw new Error(`Failed to load report (${res.status})`);
        const body = await res.json();
        const report = (body.data ?? body) as SavedReport;

        if (cancelled) return;

        setSaved(report);
        setDefinition({
          ...report.definition,
          title: report.title,
          description: report.description,
        });
      } catch (err) {
        if (!cancelled) {
          setLoadError(err instanceof Error ? err.message : 'Failed to load report');
        }
      } finally {
        if (!cancelled) setLoadingReport(false);
      }
    })();

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [session?.access_token, params.id]);

  // ---- Auto-run if stale (> 1 hour) or ?run=true ----
  useEffect(() => {
    if (hasAutoRun.current || !saved || loadingReport) return;

    const forceRun = searchParams.get('run') === 'true';
    const stale =
      !saved.last_run_at ||
      Date.now() - new Date(saved.last_run_at).getTime() > 3_600_000; // 1 hour

    if (forceRun || stale) {
      hasAutoRun.current = true;
      runQuery();
    }
  }, [saved, loadingReport, searchParams, runQuery]);

  // ---- Toast on query error ----
  useEffect(() => {
    if (error && error !== 'RATE_LIMIT') {
      toast.error(`Query failed: ${error}`);
    }
  }, [error]);

  // ---- Handlers ----
  const handleDefinitionChange = useCallback(
    (next: ReportDefinition) => {
      setDefinition(next);
      setDrawerOpen(false);
    },
    [setDefinition],
  );

  const handleSave = useCallback(async () => {
    if (!saved) return;
    setSaving(true);
    try {
      const updated = await updateReport(saved.id);
      if (updated) {
        setSaved(updated);
        toast.success('Report saved');
      }
    } finally {
      setSaving(false);
    }
  }, [saved, updateReport]);

  // ---- Loading state ----
  if (loadingReport) {
    return (
      <div className="flex flex-col h-[calc(100vh-5rem)] gap-4">
        <div className="flex items-center gap-3 pb-4 border-b border-slate-800">
          <Skeleton className="h-9 w-9" />
          <Skeleton className="h-7 w-64" />
          <div className="ml-auto flex gap-2">
            <Skeleton className="h-8 w-20" />
            <Skeleton className="h-8 w-20" />
          </div>
        </div>
        <div className="flex flex-1 gap-4 min-h-0">
          <Skeleton className="w-[300px] rounded-xl" />
          <Skeleton className="flex-1 rounded-xl" />
        </div>
      </div>
    );
  }

  // ---- Error state ----
  if (loadError) {
    return (
      <div className="flex flex-col items-center justify-center h-[calc(100vh-5rem)] gap-4">
        <p className="text-red-400">{loadError}</p>
        <Button asChild variant="outline">
          <Link href="/dashboard/reports">Back to Reports</Link>
        </Button>
      </div>
    );
  }

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
          onChange={(e) =>
            handleDefinitionChange({ ...definition, title: e.target.value })
          }
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
            Save Changes
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
          <div
            className="absolute inset-0 bg-black/60"
            onClick={() => setDrawerOpen(false)}
          />
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
