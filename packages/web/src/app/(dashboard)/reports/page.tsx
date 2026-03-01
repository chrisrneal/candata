'use client';

import Link from 'next/link';
import { Plus, FileBarChart } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { useAuth } from '@/hooks/use-auth';
import { useReports } from '@/hooks/useReports';
import { SavedReportCard } from '@/components/reports/SavedReportCard';

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-slate-700 bg-slate-900/50 py-16 px-6 text-center">
      <div className="rounded-full bg-slate-800 p-4 mb-4">
        <FileBarChart className="h-8 w-8 text-slate-400" />
      </div>
      <h3 className="text-lg font-semibold text-slate-200 mb-1">No saved reports yet</h3>
      <p className="text-sm text-slate-400 mb-6 max-w-sm">
        Build custom reports by combining indicators, housing, procurement, and trade data
        into chart-ready views you can save and revisit.
      </p>
      <Button asChild>
        <Link href="/dashboard/reports/new">
          <Plus className="h-4 w-4 mr-2" />
          Create your first report
        </Link>
      </Button>
    </div>
  );
}

function ReportCardSkeleton() {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900 p-5 space-y-3">
      <div className="flex justify-between">
        <Skeleton className="h-5 w-40" />
        <Skeleton className="h-5 w-20 rounded-full" />
      </div>
      <Skeleton className="h-4 w-full" />
      <Skeleton className="h-4 w-3/5" />
      <div className="flex items-center gap-3 pt-1">
        <Skeleton className="h-3 w-24" />
        <Skeleton className="h-3 w-12" />
      </div>
      <div className="flex gap-2 pt-2 border-t border-slate-800">
        <Skeleton className="h-8 flex-1 rounded-md" />
        <Skeleton className="h-8 flex-1 rounded-md" />
        <Skeleton className="h-8 w-8 rounded-md" />
      </div>
    </div>
  );
}

function LoadingSkeleton() {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
      {Array.from({ length: 3 }).map((_, i) => (
        <ReportCardSkeleton key={i} />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function ReportsPage() {
  const { session } = useAuth();
  const { reports, isLoading, error, deleteReport } = useReports();

  const handleDelete = async (id: string) => {
    try {
      await deleteReport(id);
      toast.success('Report deleted');
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to delete report');
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-50">Report Builder</h1>
          <p className="text-slate-400 mt-1">
            Create and manage custom data reports.
          </p>
        </div>
        <Button asChild>
          <Link href="/dashboard/reports/new">
            <Plus className="h-4 w-4 mr-2" />
            New Report
          </Link>
        </Button>
      </div>

      {/* Content */}
      {!session ? (
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-8 text-center">
          <p className="text-slate-400">Sign in to create and manage reports.</p>
        </div>
      ) : isLoading ? (
        <LoadingSkeleton />
      ) : error ? (
        <div className="rounded-xl border border-red-900/50 bg-red-950/20 p-6 text-center">
          <p className="text-red-400">Failed to load reports. Please try again.</p>
        </div>
      ) : !reports || reports.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {reports.map((report) => (
            <SavedReportCard
              key={report.id}
              report={report}
              onDelete={handleDelete}
            />
          ))}
        </div>
      )}
    </div>
  );
}
