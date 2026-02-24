import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

export async function GET(request: NextRequest) {
  const requestUrl = new URL(request.url);
  const code = requestUrl.searchParams.get('code');

  if (code) {
    // In production, exchange the code for a session via Supabase auth
    // const supabase = createRouteHandlerClient({ cookies });
    // await supabase.auth.exchangeCodeForSession(code);
  }

  return NextResponse.redirect(new URL('/overview', requestUrl.origin));
}
