import type { Metadata } from 'next';
import { TIERS } from '@candata/shared';

export const metadata: Metadata = {
  title: 'Billing',
};

const plans = Object.entries(TIERS).map(([key, tier]) => ({
  key,
  name: typeof tier === 'string' ? tier : key,
  price: key === 'free' ? '$0' : key === 'pro' ? '$49' : '$199',
  period: '/month',
  features:
    key === 'free'
      ? ['1,000 API calls/month', 'Basic indicators', 'Community support']
      : key === 'pro'
        ? ['50,000 API calls/month', 'All indicators', 'Housing data', 'Email support']
        : ['Unlimited API calls', 'All data sets', 'Priority support', 'Custom exports'],
}));

export default function BillingPage() {
  return (
    <div className="space-y-6 max-w-4xl">
      <div>
        <p className="text-sm text-slate-400">Settings</p>
        <h1 className="text-2xl font-bold text-slate-50">Billing</h1>
        <p className="text-slate-400 mt-1">Choose a plan that fits your needs.</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {plans.map((plan) => (
          <div
            key={plan.key}
            className={`rounded-lg border p-6 ${
              plan.key === 'pro'
                ? 'border-blue-600 bg-slate-900'
                : 'border-slate-800 bg-slate-900'
            }`}
          >
            <h3 className="text-lg font-semibold text-slate-50 capitalize">{plan.key}</h3>
            <div className="mt-2">
              <span className="text-3xl font-bold text-slate-50">{plan.price}</span>
              <span className="text-slate-400">{plan.period}</span>
            </div>
            <ul className="mt-4 space-y-2">
              {plan.features.map((f) => (
                <li key={f} className="flex items-center gap-2 text-sm text-slate-300">
                  <span className="text-emerald-400">&#10003;</span>
                  {f}
                </li>
              ))}
            </ul>
            <button
              className={`mt-6 w-full rounded-md py-2 text-sm font-medium transition-colors ${
                plan.key === 'pro'
                  ? 'bg-blue-600 text-white hover:bg-blue-700'
                  : 'bg-slate-800 text-slate-50 hover:bg-slate-700'
              }`}
            >
              {plan.key === 'free' ? 'Current Plan' : 'Upgrade'}
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
