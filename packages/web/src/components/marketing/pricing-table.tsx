import { Check } from 'lucide-react';
import Link from 'next/link';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

const tiers = [
  {
    name: 'Free',
    price: '$0',
    period: '/mo',
    description: 'For exploration and learning.',
    features: [
      '100 API requests/day',
      'Economic indicators',
      'Monthly data only',
      'Community support',
    ],
    cta: 'Get Started',
    href: '/signup',
    highlighted: false,
  },
  {
    name: 'Starter',
    price: '$29',
    period: '/mo',
    description: 'For individual analysts.',
    features: [
      '5,000 API requests/day',
      'All indicators + housing',
      'Daily data granularity',
      'CSV export',
      'Email support',
    ],
    cta: 'Start Free Trial',
    href: '/signup?plan=starter',
    highlighted: false,
  },
  {
    name: 'Pro',
    price: '$99',
    period: '/mo',
    description: 'For teams and professionals.',
    features: [
      '50,000 API requests/day',
      'All data sources',
      'Real-time updates',
      'Bulk export',
      'Webhook notifications',
      'Priority support',
    ],
    cta: 'Start Free Trial',
    href: '/signup?plan=pro',
    highlighted: true,
  },
  {
    name: 'Business',
    price: '$249',
    period: '/mo',
    description: 'For organizations at scale.',
    features: [
      'Unlimited API requests',
      'All data sources',
      'Real-time updates',
      'Custom integrations',
      'SLA guarantee',
      'Dedicated support',
      'SSO & team management',
    ],
    cta: 'Contact Sales',
    href: '/contact',
    highlighted: false,
  },
];

export function PricingTable() {
  return (
    <section className="py-24">
      <div className="container mx-auto max-w-screen-2xl px-4">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-slate-100">
            Simple, transparent pricing
          </h2>
          <p className="mt-4 text-slate-400">
            Start free and scale as your data needs grow.
          </p>
        </div>
        <div className="mx-auto mt-16 grid max-w-6xl gap-6 sm:grid-cols-2 lg:grid-cols-4">
          {tiers.map((tier) => (
            <Card
              key={tier.name}
              className={cn(
                'flex flex-col',
                tier.highlighted && 'border-brand-500 ring-1 ring-brand-500'
              )}
            >
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg">{tier.name}</CardTitle>
                  {tier.highlighted && <Badge>Recommended</Badge>}
                </div>
                <CardDescription>{tier.description}</CardDescription>
              </CardHeader>
              <CardContent className="flex-1">
                <div className="mb-6">
                  <span className="text-3xl font-bold text-slate-100">{tier.price}</span>
                  <span className="text-sm text-slate-400">{tier.period}</span>
                </div>
                <ul className="space-y-2">
                  {tier.features.map((feature) => (
                    <li key={feature} className="flex items-center gap-2 text-sm text-slate-300">
                      <Check className="h-4 w-4 shrink-0 text-emerald-400" />
                      {feature}
                    </li>
                  ))}
                </ul>
              </CardContent>
              <CardFooter>
                <Button
                  variant={tier.highlighted ? 'default' : 'outline'}
                  className="w-full"
                  asChild
                >
                  <Link href={tier.href}>{tier.cta}</Link>
                </Button>
              </CardFooter>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
}
