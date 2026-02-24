import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  transpilePackages: ['@candata/shared'],
  experimental: {
    serverComponentsExternalPackages: [],
  },
};

export default nextConfig;
