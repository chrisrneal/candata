import type { Metadata } from 'next';
import { PROVINCES, INDICATOR_IDS } from '@candata/shared';

export const metadata: Metadata = {
  title: 'Overview',
};

const stats = [
  { label: 'Provinces & Territories', value: Object.keys(PROVINCES).length },
  { label: 'Economic Indicators', value: INDICATOR_IDS.length },
  { label: 'Data Points', value: '2.4M+' },
  { label: 'Last Updated', value: 'Today' },
];

export default function OverviewPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-50">Overview</h1>
        <p className="text-slate-400 mt-1">Key metrics across the Canadian economy.</p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map((stat) => (
          <div
            key={stat.label}
            className="rounded-lg border border-slate-800 bg-slate-900 p-6"
          >
            <p className="text-sm text-slate-400">{stat.label}</p>
            <p className="text-2xl font-semibold text-slate-50 mt-1">{stat.value}</p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <h2 className="text-lg font-semibold text-slate-50 mb-4">CPI Trend</h2>
          <div className="h-64 flex items-center justify-center text-slate-500">
            Chart placeholder
          </div>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <h2 className="text-lg font-semibold text-slate-50 mb-4">Housing Starts</h2>
          <div className="h-64 flex items-center justify-center text-slate-500">
            Chart placeholder
          </div>
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Recent Activity</h2>
        <div className="space-y-3">
          {['CPI data updated for January 2026', 'New procurement tenders published', 'Housing starts report released', 'Trade balance data refreshed'].map(
            (item) => (
              <div key={item} className="flex items-center gap-3 text-sm text-slate-300">
                <span className="h-2 w-2 rounded-full bg-emerald-500 shrink-0" />
                {item}
              </div>
            ),
          )}
        </div>
      </div>
    </div>
  );
}
