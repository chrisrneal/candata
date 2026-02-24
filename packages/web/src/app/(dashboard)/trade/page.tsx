import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Trade',
};

const mockTradeData = [
  { partner: 'United States', exports: 425_000, imports: 380_000 },
  { partner: 'China', exports: 28_000, imports: 75_000 },
  { partner: 'United Kingdom', exports: 18_500, imports: 12_800 },
  { partner: 'Japan', exports: 13_200, imports: 16_400 },
  { partner: 'Mexico', exports: 8_900, imports: 22_100 },
  { partner: 'Germany', exports: 5_600, imports: 18_300 },
  { partner: 'South Korea', exports: 7_100, imports: 9_800 },
];

export default function TradePage() {
  const totalExports = mockTradeData.reduce((sum, d) => sum + d.exports, 0);
  const totalImports = mockTradeData.reduce((sum, d) => sum + d.imports, 0);
  const balance = totalExports - totalImports;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-50">Trade</h1>
        <p className="text-slate-400 mt-1">Canadian international trade flows.</p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">Total Exports</p>
          <p className="text-2xl font-semibold text-slate-50 mt-1">
            ${(totalExports / 1000).toFixed(1)}B
          </p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">Total Imports</p>
          <p className="text-2xl font-semibold text-slate-50 mt-1">
            ${(totalImports / 1000).toFixed(1)}B
          </p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">Trade Balance</p>
          <p
            className={`text-2xl font-semibold mt-1 ${
              balance >= 0 ? 'text-emerald-400' : 'text-red-400'
            }`}
          >
            {balance >= 0 ? '+' : '-'}${(Math.abs(balance) / 1000).toFixed(1)}B
          </p>
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Trade Balance Trend</h2>
        <div className="h-64 flex items-center justify-center text-slate-500">
          Chart placeholder
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 overflow-hidden">
        <h2 className="text-lg font-semibold text-slate-50 p-4 bg-slate-900 border-b border-slate-800">
          By Trading Partner ($ millions)
        </h2>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 bg-slate-900/50">
              <th className="text-left p-3 text-slate-400 font-medium">Partner</th>
              <th className="text-right p-3 text-slate-400 font-medium">Exports</th>
              <th className="text-right p-3 text-slate-400 font-medium">Imports</th>
              <th className="text-right p-3 text-slate-400 font-medium">Balance</th>
            </tr>
          </thead>
          <tbody>
            {mockTradeData.map((row) => {
              const bal = row.exports - row.imports;
              return (
                <tr key={row.partner} className="border-b border-slate-800/50 hover:bg-slate-900/50">
                  <td className="p-3 text-slate-50">{row.partner}</td>
                  <td className="p-3 text-right text-slate-300">
                    ${row.exports.toLocaleString()}
                  </td>
                  <td className="p-3 text-right text-slate-300">
                    ${row.imports.toLocaleString()}
                  </td>
                  <td
                    className={`p-3 text-right ${
                      bal >= 0 ? 'text-emerald-400' : 'text-red-400'
                    }`}
                  >
                    {bal >= 0 ? '+' : ''}${bal.toLocaleString()}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
