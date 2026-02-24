import Link from 'next/link';
import { ArrowRight } from 'lucide-react';
import { Button } from '@/components/ui/button';

export function CtaSection() {
  return (
    <section className="border-t border-slate-800 bg-slate-950 py-24">
      <div className="container mx-auto max-w-screen-2xl px-4 text-center">
        <h2 className="text-3xl font-bold tracking-tight text-slate-100">
          Start exploring Canadian data today
        </h2>
        <p className="mx-auto mt-4 max-w-xl text-slate-400">
          Join analysts, researchers, and developers using candata to understand the Canadian economy.
          Free tier available with no credit card required.
        </p>
        <div className="mt-8">
          <Button size="lg" asChild>
            <Link href="/signup">
              Sign Up Free
              <ArrowRight className="ml-2 h-4 w-4" />
            </Link>
          </Button>
        </div>
      </div>
    </section>
  );
}
