import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Housing API',
};

export default function HousingDocsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-slate-50">Housing API</h1>
        <p className="text-slate-400 mt-2">
          Access CMHC housing data including vacancy rates, average rents, and housing starts.
        </p>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6 space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">Vacancy Rates</h2>
        <div className="flex items-center gap-2">
          <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">GET</span>
          <code className="text-sm text-slate-50 font-mono">/v1/housing/vacancy-rates</code>
        </div>
        <p className="text-sm text-slate-300">Returns vacancy rate data by CMA and bedroom type.</p>
        <h3 className="text-sm font-medium text-slate-400">Query Parameters</h3>
        <div className="space-y-2 text-sm">
          {[
            { name: 'cma', type: 'string', desc: 'CMA code (e.g., 0580 for Toronto)' },
            { name: 'bedroom_type', type: 'string', desc: 'bachelor, 1br, 2br, 3br' },
            { name: 'year', type: 'integer', desc: 'Filter by year' },
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
        <h2 className="text-xl font-semibold text-slate-50">Average Rents</h2>
        <div className="flex items-center gap-2">
          <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">GET</span>
          <code className="text-sm text-slate-50 font-mono">/v1/housing/average-rents</code>
        </div>
        <p className="text-sm text-slate-300">Returns average rent data by CMA and bedroom type.</p>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6 space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">Housing Starts</h2>
        <div className="flex items-center gap-2">
          <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">GET</span>
          <code className="text-sm text-slate-50 font-mono">/v1/housing/starts</code>
        </div>
        <p className="text-sm text-slate-300">Returns housing starts data by CMA and dwelling type.</p>
      </div>
    </div>
  );
}
