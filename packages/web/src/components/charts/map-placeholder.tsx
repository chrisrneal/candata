import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

export function MapPlaceholder() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Canada Map</CardTitle>
      </CardHeader>
      <CardContent className="flex items-center justify-center py-12">
        <div className="text-center">
          <svg
            viewBox="0 0 200 120"
            className="mx-auto mb-4 h-24 w-40 text-slate-600"
            fill="none"
            stroke="currentColor"
            strokeWidth="1"
          >
            <path d="M20,80 L30,60 L40,65 L50,50 L55,55 L65,40 L75,45 L80,35 L90,40 L100,30 L110,35 L120,25 L130,30 L135,40 L145,35 L150,45 L160,40 L165,50 L175,55 L180,70 L170,75 L160,80 L150,78 L140,82 L130,80 L120,85 L110,80 L100,85 L90,82 L80,85 L70,80 L60,82 L50,85 L40,80 L30,82 Z" />
          </svg>
          <p className="text-sm text-slate-500">Map visualization coming soon</p>
        </div>
      </CardContent>
    </Card>
  );
}
