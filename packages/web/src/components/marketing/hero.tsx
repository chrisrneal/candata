import Link from 'next/link';
import { ArrowRight, BookOpen } from 'lucide-react';
import { Button } from '@/components/ui/button';

export function Hero() {
  return (
    <section className="relative overflow-hidden bg-gradient-to-b from-slate-900 via-slate-900 to-slate-950 py-24 sm:py-32">
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-brand-900/20 via-transparent to-transparent" />
      <div className="container relative mx-auto max-w-screen-2xl px-4">
        <div className="mx-auto max-w-3xl text-center">
          <h1 className="text-4xl font-bold tracking-tight text-slate-100 sm:text-6xl">
            Canadian Data{' '}
            <span className="bg-gradient-to-r from-brand-400 to-emerald-400 bg-clip-text text-transparent">
              Intelligence
            </span>
          </h1>
          <p className="mt-6 text-lg leading-8 text-slate-400">
            Access economic indicators, housing market data, procurement records, and trade flows
            across Canada. One API, one platform, all the data you need.
          </p>
          <div className="mt-10 flex items-center justify-center gap-x-4">
            <Button size="lg" asChild>
              <Link href="/signup">
                Get Started
                <ArrowRight className="ml-2 h-4 w-4" />
              </Link>
            </Button>
            <Button variant="outline" size="lg" asChild>
              <Link href="/docs">
                <BookOpen className="mr-2 h-4 w-4" />
                View Docs
              </Link>
            </Button>
          </div>
        </div>
      </div>
    </section>
  );
}
