import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Procurement API',
};

export default function ProcurementDocsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-slate-50">Procurement API</h1>
        <p className="text-slate-400 mt-2">
          Search and browse federal government contracts and open tenders.
        </p>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6 space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">List Contracts</h2>
        <div className="flex items-center gap-2">
          <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">GET</span>
          <code className="text-sm text-slate-50 font-mono">/v1/procurement/contracts</code>
        </div>
        <h3 className="text-sm font-medium text-slate-400">Query Parameters</h3>
        <div className="space-y-2 text-sm">
          {[
            { name: 'vendor', type: 'string', desc: 'Filter by vendor name' },
            { name: 'department', type: 'string', desc: 'Filter by department code' },
            { name: 'min_value', type: 'number', desc: 'Minimum contract value' },
            { name: 'max_value', type: 'number', desc: 'Maximum contract value' },
          ].map((param) => (
            <div key={param.name} className="flex items-start gap-4 text-slate-300">
              <code className="text-emerald-400 font-mono shrink-0">{param.name}</code>
              <span className="text-slate-500">{param.type}</span>
              <span>{param.desc}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6 space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">List Tenders</h2>
        <div className="flex items-center gap-2">
          <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">GET</span>
          <code className="text-sm text-slate-50 font-mono">/v1/procurement/tenders</code>
        </div>
        <p className="text-sm text-slate-300">Returns currently open tenders with closing dates.</p>
      </div>
    </div>
  );
}
