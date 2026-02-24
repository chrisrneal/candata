import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Indicators API',
};

export default function IndicatorsDocsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-slate-50">Indicators API</h1>
        <p className="text-slate-400 mt-2">
          Access Canadian economic indicators including CPI, GDP, unemployment, and more.
        </p>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6 space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">List Indicators</h2>
        <div className="flex items-center gap-2">
          <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">
            GET
          </span>
          <code className="text-sm text-slate-50 font-mono">/v1/indicators</code>
        </div>
        <h3 className="text-sm font-medium text-slate-400">Query Parameters</h3>
        <div className="space-y-2 text-sm">
          {[
            { name: 'page', type: 'integer', desc: 'Page number (default: 1)' },
            { name: 'per_page', type: 'integer', desc: 'Items per page (default: 25, max: 100)' },
            { name: 'frequency', type: 'string', desc: 'Filter by frequency (monthly, quarterly, annual)' },
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
        <h2 className="text-xl font-semibold text-slate-50">Get Indicator Values</h2>
        <div className="flex items-center gap-2">
          <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">
            GET
          </span>
          <code className="text-sm text-slate-50 font-mono">/v1/indicators/:id/values</code>
        </div>
        <h3 className="text-sm font-medium text-slate-400">Query Parameters</h3>
        <div className="space-y-2 text-sm">
          {[
            { name: 'geo', type: 'string', desc: 'Geography code (e.g., CA, ON, BC)' },
            { name: 'start_date', type: 'string', desc: 'Start date (YYYY-MM-DD)' },
            { name: 'end_date', type: 'string', desc: 'End date (YYYY-MM-DD)' },
          ].map((param) => (
            <div key={param.name} className="flex items-start gap-4 text-slate-300">
              <code className="text-emerald-400 font-mono shrink-0">{param.name}</code>
              <span className="text-slate-500">{param.type}</span>
              <span>{param.desc}</span>
            </div>
          ))}
        </div>
        <h3 className="text-sm font-medium text-slate-400 mt-4">Example Response</h3>
        <pre className="rounded-md bg-slate-800 p-4 text-sm text-slate-300 font-mono overflow-x-auto">
{`{
  "data": [
    {
      "id": "cpi_all_items",
      "date": "2025-11-01",
      "value": 162.3,
      "geo": "CA"
    }
  ],
  "meta": { "page": 1, "per_page": 25, "total": 120 }
}`}
        </pre>
      </div>
    </div>
  );
}
