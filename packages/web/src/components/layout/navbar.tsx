'use client';

import { useState } from 'react';
import Link from 'next/link';
import { BarChart3, LogIn, UserPlus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { MobileNav } from './mobile-nav';

export function Navbar() {
  return (
    <header className="sticky top-0 z-50 w-full border-b border-slate-800 bg-slate-900/95 backdrop-blur supports-[backdrop-filter]:bg-slate-900/60">
      <div className="container flex h-14 max-w-screen-2xl items-center">
        <Link href="/" className="mr-6 flex items-center space-x-2">
          <BarChart3 className="h-6 w-6 text-brand-500" />
          <span className="font-bold text-slate-100">candata</span>
        </Link>

        <nav className="hidden items-center gap-6 text-sm md:flex">
          <Link href="/dashboard" className="text-slate-400 transition-colors hover:text-slate-100">
            Dashboard
          </Link>
          <Link href="/docs" className="text-slate-400 transition-colors hover:text-slate-100">
            Docs
          </Link>
          <Link href="/pricing" className="text-slate-400 transition-colors hover:text-slate-100">
            Pricing
          </Link>
        </nav>

        <div className="flex flex-1 items-center justify-end space-x-2">
          <nav className="hidden items-center space-x-2 md:flex">
            <Button variant="ghost" size="sm" asChild>
              <Link href="/login">
                <LogIn className="mr-2 h-4 w-4" />
                Login
              </Link>
            </Button>
            <Button size="sm" asChild>
              <Link href="/signup">
                <UserPlus className="mr-2 h-4 w-4" />
                Sign Up
              </Link>
            </Button>
          </nav>
          <MobileNav />
        </div>
      </div>
    </header>
  );
}
