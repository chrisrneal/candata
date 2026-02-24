import type { Metadata } from 'next';
import Link from 'next/link';
import { CMA_CODES } from '@candata/shared';

export const metadata: Metadata = {
  title: 'Housing',
};

const cmaEntries = Object.entries(CMA_CODES).map(([code, name]) => ({
  code,
  name,
  vacancyRate: (Math.random() * 5 + 0.5).toFixed(1),
  avgRent: Math.round(1000 + Math.random() * 1500),
  starts: Math.round(500 + Math.random() * 5000),
}));

export default function HousingPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-50">Housing</h1>
        <p className="text-slate-400 mt-1">
          Housing data across Census Metropolitan Areas.
        </p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">CMAs Tracked</p>
          <p className="text-2xl font-semibold text-slate-50 mt-1">
            {cmaEntries.length}
          </p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">Avg Vacancy Rate</p>
          <p className="text-2xl font-semibold text-slate-50 mt-1">2.8%</p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">Avg Rent (National)</p>
          <p className="text-2xl font-semibold text-slate-50 mt-1">$1,850</p>
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 bg-slate-900">
              <th className="text-left p-3 text-slate-400 font-medium">CMA</th>
              <th className="text-right p-3 text-slate-400 font-medium">Vacancy Rate</th>
              <th className="text-right p-3 text-slate-400 font-medium">Avg Rent</th>
              <th className="text-right p-3 text-slate-400 font-medium">Starts</th>
            </tr>
          </thead>
          <tbody>
            {cmaEntries.map((cma) => (
              <tr key={cma.code} className="border-b border-slate-800/50 hover:bg-slate-900/50">
                <td className="p-3">
                  <Link
                    href={`/housing/${cma.code}`}
                    className="text-slate-50 hover:text-blue-400 transition-colors"
                  >
                    {cma.name}
                  </Link>
                </td>
                <td className="p-3 text-right text-slate-300">{cma.vacancyRate}%</td>
                <td className="p-3 text-right text-slate-300">${cma.avgRent.toLocaleString()}</td>
                <td className="p-3 text-right text-slate-300">{cma.starts.toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
