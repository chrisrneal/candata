/** @type {import('next').NextConfig} */
const nextConfig = {
  transpilePackages: ['@candata/shared'],
  experimental: {
    serverComponentsExternalPackages: [],
  },
};

export default nextConfig;
