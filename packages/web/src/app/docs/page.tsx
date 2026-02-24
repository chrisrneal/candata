import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'API Documentation',
};

export default function DocsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-slate-50">API Documentation</h1>
        <p className="text-slate-400 mt-2 text-lg">
          Access Canadian economic, housing, procurement, and trade data through our REST API.
        </p>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
        <h2 className="text-xl font-semibold text-slate-50">Base URL</h2>
        <code className="mt-2 block rounded-md bg-slate-800 px-4 py-3 text-sm text-emerald-400 font-mono">
          https://api.candata.ca/v1
        </code>
      </div>

      <div className="space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">Available Endpoints</h2>
        <div className="space-y-3">
          {[
            { method: 'GET', path: '/indicators', description: 'List all economic indicators' },
            { method: 'GET', path: '/indicators/:id/values', description: 'Get time series data for an indicator' },
            { method: 'GET', path: '/housing/vacancy-rates', description: 'Get vacancy rates by CMA' },
            { method: 'GET', path: '/housing/average-rents', description: 'Get average rents by CMA' },
            { method: 'GET', path: '/procurement/contracts', description: 'Search federal contracts' },
            { method: 'GET', path: '/procurement/tenders', description: 'Browse open tenders' },
            { method: 'GET', path: '/trade/flows', description: 'Get trade flow data' },
          ].map((endpoint) => (
            <div
              key={endpoint.path}
              className="flex items-center gap-3 rounded-md border border-slate-800 bg-slate-900/50 px-4 py-3"
            >
              <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">
                {endpoint.method}
              </span>
              <code className="text-sm text-slate-50 font-mono">{endpoint.path}</code>
              <span className="text-sm text-slate-400 ml-auto">{endpoint.description}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
        <h2 className="text-xl font-semibold text-slate-50">Response Format</h2>
        <p className="text-slate-400 mt-2">
          All endpoints return JSON responses wrapped in a standard envelope.
        </p>
        <pre className="mt-4 rounded-md bg-slate-800 p-4 text-sm text-slate-300 font-mono overflow-x-auto">
{`{
  "data": [...],
  "meta": {
    "page": 1,
    "per_page": 25,
    "total": 100
  },
  "links": {
    "self": "/v1/indicators?page=1",
    "next": "/v1/indicators?page=2"
  }
}`}
        </pre>
      </div>
    </div>
  );
}
