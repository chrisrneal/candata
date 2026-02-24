import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'candata — Canadian Data Intelligence',
  description: 'Canadian economic, housing, and procurement data intelligence platform.',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
