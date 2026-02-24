import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

interface Param {
  name: string;
  type: string;
  required?: boolean;
  description: string;
}

interface EndpointCardProps {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  path: string;
  description: string;
  params?: Param[];
}

const methodColors: Record<string, string> = {
  GET: 'bg-emerald-600',
  POST: 'bg-blue-600',
  PUT: 'bg-amber-600',
  DELETE: 'bg-red-600',
};

export function EndpointCard({ method, path, description, params }: EndpointCardProps) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-3">
          <Badge className={methodColors[method]}>{method}</Badge>
          <code className="text-sm font-mono text-slate-200">{path}</code>
        </div>
        <CardDescription className="mt-2">{description}</CardDescription>
      </CardHeader>
      {params && params.length > 0 && (
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Parameter</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Required</TableHead>
                <TableHead>Description</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {params.map((param) => (
                <TableRow key={param.name}>
                  <TableCell className="font-mono text-xs">{param.name}</TableCell>
                  <TableCell className="text-xs text-slate-400">{param.type}</TableCell>
                  <TableCell>
                    {param.required ? (
                      <Badge variant="outline" className="text-xs">required</Badge>
                    ) : (
                      <span className="text-xs text-slate-500">optional</span>
                    )}
                  </TableCell>
                  <TableCell className="text-xs">{param.description}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      )}
    </Card>
  );
}
