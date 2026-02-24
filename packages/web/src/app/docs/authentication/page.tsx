import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Authentication',
};

export default function AuthDocsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-slate-50">Authentication</h1>
        <p className="text-slate-400 mt-2">
          Authenticate your API requests using API keys or OAuth tokens.
        </p>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6 space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">API Key Authentication</h2>
        <p className="text-slate-300 text-sm">
          Include your API key in the <code className="text-emerald-400">Authorization</code> header.
        </p>
        <pre className="rounded-md bg-slate-800 p-4 text-sm text-slate-300 font-mono overflow-x-auto">
{`curl -H "Authorization: Bearer sk-your-api-key" \\
  https://api.candata.ca/v1/indicators`}
        </pre>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6 space-y-4">
        <h2 className="text-xl font-semibold text-slate-50">Rate Limits</h2>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between text-slate-300">
            <span>Free Tier</span>
            <span>1,000 requests/month</span>
          </div>
          <div className="flex justify-between text-slate-300">
            <span>Pro Tier</span>
            <span>50,000 requests/month</span>
          </div>
          <div className="flex justify-between text-slate-300">
            <span>Enterprise Tier</span>
            <span>Unlimited</span>
          </div>
        </div>
      </div>
    </div>
  );
}
