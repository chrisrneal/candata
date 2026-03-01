import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import { Toaster } from 'sonner';
import { Providers } from '@/components/providers';
import '@/app/globals.css';

const inter = Inter({ subsets: ['latin'], variable: '--font-inter' });

export const metadata: Metadata = {
  title: { default: 'candata', template: '%s | candata' },
  description: 'Canadian economic, housing, and procurement data intelligence platform.',
  openGraph: {
    title: 'candata',
    description: 'Canadian Data Intelligence Platform',
    type: 'website',
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.variable} font-sans bg-slate-950 text-slate-50 antialiased`}>
        <Providers>{children}</Providers>
        <Toaster
          theme="dark"
          toastOptions={{
            style: {
              background: 'rgb(15 23 42)',
              border: '1px solid rgb(30 41 59)',
              color: 'rgb(226 232 240)',
            },
          }}
        />
      </body>
    </html>
  );
}
