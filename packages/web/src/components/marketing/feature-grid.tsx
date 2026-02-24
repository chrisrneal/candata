import { TrendingUp, Building2, FileText, Ship } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';

const features = [
  {
    title: 'Economic Indicators',
    description: 'GDP, CPI, employment, interest rates, and exchange rates from Statistics Canada and the Bank of Canada.',
    icon: TrendingUp,
  },
  {
    title: 'Housing Market',
    description: 'Vacancy rates, average rents, and housing starts from CMHC across all major census metropolitan areas.',
    icon: Building2,
  },
  {
    title: 'Procurement Data',
    description: 'Federal government contracts and tenders from CanadaBuys, searchable and filterable.',
    icon: FileText,
  },
  {
    title: 'Trade Flows',
    description: 'Import and export data by HS chapter and partner country, tracking Canadian trade relationships.',
    icon: Ship,
  },
];

export function FeatureGrid() {
  return (
    <section className="py-24">
      <div className="container mx-auto max-w-screen-2xl px-4">
        <div className="mx-auto max-w-2xl text-center">
          <h2 className="text-3xl font-bold tracking-tight text-slate-100">
            Comprehensive Canadian data
          </h2>
          <p className="mt-4 text-slate-400">
            Everything you need to understand the Canadian economy, all in one place.
          </p>
        </div>
        <div className="mx-auto mt-16 grid max-w-5xl gap-6 sm:grid-cols-2">
          {features.map((feature) => (
            <Card key={feature.title} className="transition-colors hover:border-slate-700">
              <CardHeader>
                <div className="mb-2 flex h-10 w-10 items-center justify-center rounded-lg bg-brand-600/10">
                  <feature.icon className="h-5 w-5 text-brand-400" />
                </div>
                <CardTitle className="text-lg">{feature.title}</CardTitle>
              </CardHeader>
              <CardContent>
                <CardDescription className="text-sm leading-relaxed">
                  {feature.description}
                </CardDescription>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
}
