import { CodeBlock } from './code-block';

interface ResponseExampleProps {
  title?: string;
  data: unknown;
}

export function ResponseExample({ title = 'Response', data }: ResponseExampleProps) {
  const formatted = JSON.stringify(data, null, 2);

  return (
    <div className="space-y-2">
      <h4 className="text-sm font-medium text-slate-400">{title}</h4>
      <CodeBlock code={formatted} language="json" />
    </div>
  );
}
