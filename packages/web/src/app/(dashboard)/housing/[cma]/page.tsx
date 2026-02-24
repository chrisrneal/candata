import type { Metadata } from 'next';
import { CMA_CODES } from '@candata/shared';

interface Props {
  params: Promise<{ cma: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { cma } = await params;
  const name = CMA_CODES[cma as keyof typeof CMA_CODES] ?? cma;
  return { title: `Housing — ${name}` };
}

export default async function CmaDetailPage({ params }: Props) {
  const { cma } = await params;
  const name = CMA_CODES[cma as keyof typeof CMA_CODES] ?? cma;

  const bedroomTypes = ['Bachelor', '1 Bedroom', '2 Bedroom', '3 Bedroom+'];
  const mockRents = bedroomTypes.map((type) => ({
    type,
    rent: Math.round(800 + Math.random() * 1800),
    vacancy: (Math.random() * 5 + 0.5).toFixed(1),
  }));

  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm text-slate-400">Housing</p>
        <h1 className="text-2xl font-bold text-slate-50">{name}</h1>
        <p className="text-slate-400 mt-1">CMA Code: {cma}</p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-4">
          <p className="text-sm text-slate-400">Vacancy Rate</p>
          <p className="text-xl font-semibold text-slate-50 mt-1">2.4%</p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-4">
          <p className="text-sm text-slate-400">Average Rent</p>
          <p className="text-xl font-semibold text-slate-50 mt-1">$1,750</p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-4">
          <p className="text-sm text-slate-400">Housing Starts (2025)</p>
          <p className="text-xl font-semibold text-slate-50 mt-1">3,240</p>
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Rent Trends</h2>
        <div className="h-64 flex items-center justify-center text-slate-500">
          Chart placeholder
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 overflow-hidden">
        <h2 className="text-lg font-semibold text-slate-50 p-4 bg-slate-900">
          By Bedroom Type
        </h2>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 bg-slate-900">
              <th className="text-left p-3 text-slate-400 font-medium">Type</th>
              <th className="text-right p-3 text-slate-400 font-medium">Avg Rent</th>
              <th className="text-right p-3 text-slate-400 font-medium">Vacancy</th>
            </tr>
          </thead>
          <tbody>
            {mockRents.map((row) => (
              <tr key={row.type} className="border-b border-slate-800/50">
                <td className="p-3 text-slate-300">{row.type}</td>
                <td className="p-3 text-right text-slate-50">${row.rent.toLocaleString()}</td>
                <td className="p-3 text-right text-slate-300">{row.vacancy}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
