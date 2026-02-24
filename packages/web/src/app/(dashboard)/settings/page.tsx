import type { Metadata } from 'next';
import Link from 'next/link';

export const metadata: Metadata = {
  title: 'Settings',
};

export default function SettingsPage() {
  return (
    <div className="space-y-6 max-w-2xl">
      <div>
        <h1 className="text-2xl font-bold text-slate-50">Settings</h1>
        <p className="text-slate-400 mt-1">Manage your account and preferences.</p>
      </div>

      <div className="space-y-4">
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <h2 className="text-lg font-semibold text-slate-50">Profile</h2>
          <p className="text-sm text-slate-400 mt-1">Update your account details.</p>
          <div className="mt-4 space-y-4">
            <div>
              <label className="block text-sm text-slate-400 mb-1">Name</label>
              <div className="rounded-md border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-slate-50">
                Demo User
              </div>
            </div>
            <div>
              <label className="block text-sm text-slate-400 mb-1">Email</label>
              <div className="rounded-md border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-slate-50">
                user@example.com
              </div>
            </div>
          </div>
        </div>

        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <h2 className="text-lg font-semibold text-slate-50">API Access</h2>
          <p className="text-sm text-slate-400 mt-1">Manage your API keys.</p>
          <div className="mt-4">
            <div className="rounded-md border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-slate-400 font-mono">
              sk-****************************a1b2
            </div>
          </div>
        </div>

        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <h2 className="text-lg font-semibold text-slate-50">Subscription</h2>
          <p className="text-sm text-slate-400 mt-1">Current plan: Free Tier</p>
          <div className="mt-4">
            <Link
              href="/settings/billing"
              className="inline-flex items-center rounded-md bg-slate-800 px-3 py-2 text-sm text-slate-50 hover:bg-slate-700 transition-colors"
            >
              Manage billing
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
