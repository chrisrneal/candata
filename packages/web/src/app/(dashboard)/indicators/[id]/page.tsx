import type { Metadata } from 'next';

interface Props {
  params: Promise<{ id: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { id } = await params;
  const name = id.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  return { title: name };
}

export default async function IndicatorDetailPage({ params }: Props) {
  const { id } = await params;
  const name = id.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());

  const mockValues = Array.from({ length: 12 }, (_, i) => ({
    date: `2025-${String(i + 1).padStart(2, '0')}`,
    value: (100 + Math.sin(i / 2) * 10 + Math.random() * 5).toFixed(1),
  }));

  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm text-slate-400">Indicator</p>
        <h1 className="text-2xl font-bold text-slate-50">{name}</h1>
        <p className="text-slate-400 mt-1">ID: {id}</p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-4">
          <p className="text-sm text-slate-400">Latest Value</p>
          <p className="text-xl font-semibold text-slate-50 mt-1">
            {mockValues[mockValues.length - 1].value}
          </p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-4">
          <p className="text-sm text-slate-400">Period</p>
          <p className="text-xl font-semibold text-slate-50 mt-1">Monthly</p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-4">
          <p className="text-sm text-slate-400">Source</p>
          <p className="text-xl font-semibold text-slate-50 mt-1">Statistics Canada</p>
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Time Series</h2>
        <div className="h-64 flex items-center justify-center text-slate-500">
          Chart placeholder
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 bg-slate-900">
              <th className="text-left p-3 text-slate-400 font-medium">Date</th>
              <th className="text-right p-3 text-slate-400 font-medium">Value</th>
            </tr>
          </thead>
          <tbody>
            {mockValues.reverse().map((row) => (
              <tr key={row.date} className="border-b border-slate-800/50">
                <td className="p-3 text-slate-300">{row.date}</td>
                <td className="p-3 text-right text-slate-50">{row.value}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
