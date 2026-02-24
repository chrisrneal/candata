'use client';

import { useState } from 'react';
import Link from 'next/link';
import { Menu, BarChart3, LogIn, UserPlus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from '@/components/ui/sheet';
import { Separator } from '@/components/ui/separator';

const navLinks = [
  { href: '/dashboard', label: 'Dashboard' },
  { href: '/docs', label: 'Docs' },
  { href: '/pricing', label: 'Pricing' },
];

export function MobileNav() {
  const [open, setOpen] = useState(false);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <Button variant="ghost" size="icon" className="md:hidden">
          <Menu className="h-5 w-5 text-slate-300" />
          <span className="sr-only">Toggle menu</span>
        </Button>
      </SheetTrigger>
      <SheetContent side="left" className="w-72">
        <SheetHeader>
          <SheetTitle className="flex items-center space-x-2">
            <BarChart3 className="h-5 w-5 text-brand-500" />
            <span>candata</span>
          </SheetTitle>
        </SheetHeader>
        <Separator className="my-4" />
        <nav className="flex flex-col space-y-3">
          {navLinks.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              onClick={() => setOpen(false)}
              className="text-sm text-slate-300 transition-colors hover:text-slate-100"
            >
              {link.label}
            </Link>
          ))}
        </nav>
        <Separator className="my-4" />
        <div className="flex flex-col space-y-2">
          <Button variant="ghost" size="sm" asChild>
            <Link href="/login" onClick={() => setOpen(false)}>
              <LogIn className="mr-2 h-4 w-4" />
              Login
            </Link>
          </Button>
          <Button size="sm" asChild>
            <Link href="/signup" onClick={() => setOpen(false)}>
              <UserPlus className="mr-2 h-4 w-4" />
              Sign Up
            </Link>
          </Button>
        </div>
      </SheetContent>
    </Sheet>
  );
}
