import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Procurement',
};

const mockContracts = [
  { id: 'C-2025-0412', vendor: 'Deloitte Canada', value: 2_450_000, department: 'PSPC', date: '2025-11-15' },
  { id: 'C-2025-0389', vendor: 'IBM Canada Ltd', value: 5_120_000, department: 'SSC', date: '2025-11-10' },
  { id: 'C-2025-0367', vendor: 'CGI Group Inc', value: 1_890_000, department: 'CRA', date: '2025-11-08' },
  { id: 'C-2025-0345', vendor: 'Accenture Canada', value: 3_200_000, department: 'ESDC', date: '2025-10-28' },
  { id: 'C-2025-0321', vendor: 'KPMG LLP', value: 980_000, department: 'DND', date: '2025-10-22' },
];

const mockTenders = [
  { id: 'T-2025-1102', title: 'Cloud Migration Services', closing: '2025-12-15', department: 'SSC' },
  { id: 'T-2025-1098', title: 'IT Security Assessment', closing: '2025-12-10', department: 'CSE' },
  { id: 'T-2025-1085', title: 'Data Analytics Platform', closing: '2025-12-05', department: 'StatCan' },
];

export default function ProcurementPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-50">Procurement</h1>
        <p className="text-slate-400 mt-1">Federal contracts and open tenders.</p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">Total Contracts</p>
          <p className="text-2xl font-semibold text-slate-50 mt-1">12,847</p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">Open Tenders</p>
          <p className="text-2xl font-semibold text-slate-50 mt-1">342</p>
        </div>
        <div className="rounded-lg border border-slate-800 bg-slate-900 p-6">
          <p className="text-sm text-slate-400">Total Value (FY25)</p>
          <p className="text-2xl font-semibold text-slate-50 mt-1">$4.2B</p>
        </div>
      </div>

      <div className="rounded-lg border border-slate-800 overflow-hidden">
        <h2 className="text-lg font-semibold text-slate-50 p-4 bg-slate-900 border-b border-slate-800">
          Recent Contracts
        </h2>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 bg-slate-900/50">
              <th className="text-left p-3 text-slate-400 font-medium">ID</th>
              <th className="text-left p-3 text-slate-400 font-medium">Vendor</th>
              <th className="text-left p-3 text-slate-400 font-medium">Dept</th>
              <th className="text-right p-3 text-slate-400 font-medium">Value</th>
              <th className="text-right p-3 text-slate-400 font-medium">Date</th>
            </tr>
          </thead>
          <tbody>
            {mockContracts.map((c) => (
              <tr key={c.id} className="border-b border-slate-800/50 hover:bg-slate-900/50">
                <td className="p-3 text-slate-300 font-mono text-xs">{c.id}</td>
                <td className="p-3 text-slate-50">{c.vendor}</td>
                <td className="p-3 text-slate-400">{c.department}</td>
                <td className="p-3 text-right text-slate-50">
                  ${c.value.toLocaleString()}
                </td>
                <td className="p-3 text-right text-slate-400">{c.date}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="rounded-lg border border-slate-800 overflow-hidden">
        <h2 className="text-lg font-semibold text-slate-50 p-4 bg-slate-900 border-b border-slate-800">
          Open Tenders
        </h2>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 bg-slate-900/50">
              <th className="text-left p-3 text-slate-400 font-medium">ID</th>
              <th className="text-left p-3 text-slate-400 font-medium">Title</th>
              <th className="text-left p-3 text-slate-400 font-medium">Dept</th>
              <th className="text-right p-3 text-slate-400 font-medium">Closing</th>
            </tr>
          </thead>
          <tbody>
            {mockTenders.map((t) => (
              <tr key={t.id} className="border-b border-slate-800/50 hover:bg-slate-900/50">
                <td className="p-3 text-slate-300 font-mono text-xs">{t.id}</td>
                <td className="p-3 text-slate-50">{t.title}</td>
                <td className="p-3 text-slate-400">{t.department}</td>
                <td className="p-3 text-right text-slate-400">{t.closing}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
