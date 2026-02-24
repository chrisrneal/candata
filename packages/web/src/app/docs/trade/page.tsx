import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Trade API',
};

export default function TradeDocsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-slate-50">Trade API</h1>
        <p className="text-slate-400 mt-2">
          Access Canadian international trade flow data by partner country and commodity.
        </p>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6 space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">Get Trade Flows</h2>
        <div className="flex items-center gap-2">
          <span className="rounded bg-emerald-900/50 px-2 py-0.5 text-xs font-mono font-medium text-emerald-400">GET</span>
          <code className="text-sm text-slate-50 font-mono">/v1/trade/flows</code>
        </div>
        <h3 className="text-sm font-medium text-slate-400">Query Parameters</h3>
        <div className="space-y-2 text-sm">
          {[
            { name: 'direction', type: 'string', desc: 'export or import' },
            { name: 'partner', type: 'string', desc: 'Trading partner country code' },
            { name: 'commodity', type: 'string', desc: 'HS commodity code' },
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
      "date": "2025-11-01",
      "direction": "export",
      "partner": "US",
      "value": 42500000000,
      "commodity": "2709"
    }
  ]
}`}
        </pre>
      </div>
    </div>
  );
}
