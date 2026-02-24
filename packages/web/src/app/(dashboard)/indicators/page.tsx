'use client';

import type { Metadata } from 'next';
import Link from 'next/link';
import { INDICATOR_IDS, FREQUENCIES } from '@candata/shared';
import { useState } from 'react';

const indicators = INDICATOR_IDS.map((id, i) => ({
  id,
  name: id
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase()),
  frequency: FREQUENCIES[i % FREQUENCIES.length],
  latestValue: (100 + Math.random() * 50).toFixed(1),
  change: (Math.random() * 4 - 2).toFixed(2),
}));

export default function IndicatorsPage() {
  const [search, setSearch] = useState('');

  const filtered = indicators.filter((ind) =>
    ind.name.toLowerCase().includes(search.toLowerCase()),
  );

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-50">Indicators</h1>
        <p className="text-slate-400 mt-1">Browse Canadian economic indicators.</p>
      </div>

      <div>
        <input
          type="text"
          placeholder="Search indicators..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full max-w-sm rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-slate-50 placeholder:text-slate-500 focus:outline-none focus:ring-2 focus:ring-slate-600"
        />
      </div>

      <div className="rounded-lg border border-slate-800 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 bg-slate-900">
              <th className="text-left p-3 text-slate-400 font-medium">Indicator</th>
              <th className="text-left p-3 text-slate-400 font-medium">Frequency</th>
              <th className="text-right p-3 text-slate-400 font-medium">Latest</th>
              <th className="text-right p-3 text-slate-400 font-medium">Change</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((ind) => (
              <tr key={ind.id} className="border-b border-slate-800/50 hover:bg-slate-900/50">
                <td className="p-3">
                  <Link
                    href={`/indicators/${ind.id}`}
                    className="text-slate-50 hover:text-blue-400 transition-colors"
                  >
                    {ind.name}
                  </Link>
                </td>
                <td className="p-3 text-slate-400">{ind.frequency}</td>
                <td className="p-3 text-right text-slate-50">{ind.latestValue}</td>
                <td
                  className={`p-3 text-right ${
                    parseFloat(ind.change) >= 0 ? 'text-emerald-400' : 'text-red-400'
                  }`}
                >
                  {parseFloat(ind.change) >= 0 ? '+' : ''}
                  {ind.change}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
