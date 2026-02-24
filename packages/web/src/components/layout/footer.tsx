import Link from 'next/link';
import { BarChart3 } from 'lucide-react';
import { Separator } from '@/components/ui/separator';

const footerLinks = [
  { href: '/docs', label: 'Docs' },
  { href: '/docs/api', label: 'API' },
  { href: '/pricing', label: 'Pricing' },
  { href: 'https://github.com/chrisrneal/candata', label: 'GitHub' },
  { href: '/contact', label: 'Contact' },
];

export function Footer() {
  return (
    <footer className="border-t border-slate-800 bg-slate-950">
      <div className="container mx-auto max-w-screen-2xl px-4 py-8">
        <div className="flex flex-col items-center justify-between gap-4 md:flex-row">
          <div className="flex items-center space-x-2">
            <BarChart3 className="h-5 w-5 text-brand-500" />
            <span className="font-bold text-slate-100">candata</span>
          </div>

          <nav className="flex flex-wrap items-center gap-6 text-sm">
            {footerLinks.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                className="text-slate-400 transition-colors hover:text-slate-200"
              >
                {link.label}
              </Link>
            ))}
          </nav>
        </div>

        <Separator className="my-6" />

        <p className="text-center text-xs text-slate-500">
          &copy; {new Date().getFullYear()} candata. Canadian public data intelligence.
        </p>
      </div>
    </footer>
  );
}
