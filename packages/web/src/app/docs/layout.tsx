import Link from 'next/link';

const navItems = [
  { href: '/docs', label: 'Overview' },
  { href: '/docs/authentication', label: 'Authentication' },
  { href: '/docs/indicators', label: 'Indicators' },
  { href: '/docs/housing', label: 'Housing' },
  { href: '/docs/procurement', label: 'Procurement' },
  { href: '/docs/trade', label: 'Trade' },
];

export default function DocsLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen flex">
      <aside className="w-64 border-r border-slate-800 bg-slate-900 p-6 shrink-0">
        <Link href="/" className="text-lg font-bold text-slate-50">
          candata
        </Link>
        <p className="text-xs text-slate-500 mt-1">API Documentation</p>
        <nav className="mt-8 space-y-1">
          {navItems.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className="block rounded-md px-3 py-2 text-sm text-slate-300 hover:bg-slate-800 hover:text-slate-50 transition-colors"
            >
              {item.label}
            </Link>
          ))}
        </nav>
      </aside>
      <main className="flex-1 p-8 max-w-4xl">{children}</main>
    </div>
  );
}
