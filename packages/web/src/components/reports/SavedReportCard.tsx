'use client';

import { useState } from 'react';
import Link from 'next/link';
import { formatDistanceToNow } from 'date-fns';
import { Play, Pencil, Trash2, Loader2, Clock } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog';
import type { SavedReport } from '@/hooks/use-report-builder';

// ---------------------------------------------------------------------------
// Domain colours
// ---------------------------------------------------------------------------

const DOMAIN_COLORS: Record<string, string> = {
  indicators: 'bg-blue-600/20 text-blue-400 border-blue-600/30',
  housing: 'bg-emerald-600/20 text-emerald-400 border-emerald-600/30',
  procurement: 'bg-amber-600/20 text-amber-400 border-amber-600/30',
  trade: 'bg-purple-600/20 text-purple-400 border-purple-600/30',
};

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface SavedReportCardProps {
  report: SavedReport;
  onDelete: (id: string) => Promise<void>;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function SavedReportCard({ report, onDelete }: SavedReportCardProps) {
  const [deleting, setDeleting] = useState(false);
  const domainColor = DOMAIN_COLORS[report.definition.domain] ?? '';

  const relativeTime = report.last_run_at
    ? `Last run ${formatDistanceToNow(new Date(report.last_run_at), { addSuffix: true })}`
    : 'Never run';

  const handleDelete = async () => {
    setDeleting(true);
    try {
      await onDelete(report.id);
    } finally {
      setDeleting(false);
    }
  };

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900 p-5 flex flex-col gap-3 hover:border-slate-700 transition-colors group">
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <Link
          href={`/dashboard/reports/${report.id}`}
          className="font-semibold text-slate-100 line-clamp-1 hover:text-brand-400 transition-colors"
        >
          {report.title}
        </Link>
        <Badge className={domainColor}>{report.definition.domain}</Badge>
      </div>

      {/* Description */}
      {report.description && (
        <p className="text-sm text-slate-400 line-clamp-2">{report.description}</p>
      )}

      {/* Meta */}
      <div className="flex items-center gap-4 text-xs text-slate-500 mt-auto">
        <span className="flex items-center gap-1">
          <Clock className="h-3 w-3" />
          {relativeTime}
        </span>
        <span className="capitalize">{report.definition.visualization}</span>
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2 pt-1 border-t border-slate-800">
        <Button asChild variant="ghost" size="sm" className="flex-1">
          <Link href={`/dashboard/reports/${report.id}`}>
            <Pencil className="h-3.5 w-3.5 mr-1.5" />
            Edit
          </Link>
        </Button>
        <Button asChild variant="secondary" size="sm" className="flex-1">
          <Link href={`/dashboard/reports/${report.id}?run=true`}>
            <Play className="h-3.5 w-3.5 mr-1.5" />
            Run
          </Link>
        </Button>

        {/* Delete with confirmation */}
        <AlertDialog>
          <AlertDialogTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="shrink-0 text-slate-500 hover:text-red-400 hover:bg-red-950/30"
              disabled={deleting}
            >
              {deleting ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Trash2 className="h-3.5 w-3.5" />
              )}
            </Button>
          </AlertDialogTrigger>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Delete this report?</AlertDialogTitle>
              <AlertDialogDescription>
                &ldquo;{report.title}&rdquo; will be permanently deleted. This action
                cannot be undone.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>Cancel</AlertDialogCancel>
              <AlertDialogAction
                onClick={handleDelete}
                className="bg-red-600 text-white hover:bg-red-700"
              >
                {deleting ? (
                  <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
                ) : (
                  <Trash2 className="h-3.5 w-3.5 mr-1.5" />
                )}
                Delete
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </div>
    </div>
  );
}
