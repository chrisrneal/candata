# candata-web

Next.js 14 customer dashboard for the CanData platform.

## Setup

```bash
# From the repo root
cd packages/web
npm install
```

## Development

```bash
npm run dev
# → http://localhost:3000
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Framework | Next.js 14 (App Router) |
| Styling | Tailwind CSS (dark slate theme) |
| Charts | Recharts |
| Data Fetching | SWR, @tanstack/react-query |
| Auth | @supabase/auth-helpers-nextjs |
| Billing | Stripe via @stripe/react-stripe-js |
| UI Primitives | Radix UI (dialog, alert-dialog, dropdown-menu, select, tooltip) |
| Toasts | sonner |
| Icons | lucide-react |
| Shared Types | @candata/shared |

## Project Structure

```
src/
├── app/
│   ├── layout.tsx              # Root layout with Toaster provider
│   └── (dashboard)/
│       └── reports/
│           ├── page.tsx         # Saved reports listing
│           ├── new/
│           │   └── page.tsx     # Report builder (create new)
│           └── [id]/
│               └── page.tsx     # Report builder (edit existing)
├── components/
│   ├── layout/
│   │   └── sidebar.tsx          # Dashboard sidebar nav
│   ├── reports/
│   │   ├── ReportConfig.tsx     # Left-panel config sidebar (data source, geography, dates, viz)
│   │   ├── ResultsPanel.tsx     # Right-panel results (charts, table, CSV export)
│   │   └── SavedReportCard.tsx  # Card component for report listing
│   └── ui/
│       └── alert-dialog.tsx     # Radix AlertDialog (shadcn-style, dark theme)
├── hooks/
│   ├── use-report-builder.ts    # Core hook: report state, runQuery(), saveReport()
│   └── useReports.ts            # SWR hook: CRUD for saved reports with optimistic updates
├── lib/
│   └── reportMetrics.ts         # Metrics registry (18 metrics across 4 domains)
└── types/
```

## Report Builder

The report builder is a full-stack feature for creating custom data reports.

### Pages

- **`/reports`** — Lists all saved reports with search, delete, and quick-run actions. Uses `useReports` SWR hook for data fetching with optimistic deletes.
- **`/reports/new`** — Builder canvas with config sidebar + results panel. URL state sync persists `domain`, `metric`, `date_from`, `date_to`, and `group_by` as query params. Mobile-responsive with a slide-up drawer on small screens.
- **`/reports/[id]`** — Edit mode. Loads an existing report definition, pre-populates the builder, and auto-runs the query if the report is stale (>1 hour) or `?run=true` is set.

### Components

- **`ReportConfig`** — Four collapsible sections: Data Source (domain + metric), Geography (group_by + province/CMA checkboxes), Date Range (month inputs + 1y/3y/5y presets), Visualization (line/bar/area toggle).
- **`ResultsPanel`** — Displays chart (Recharts line/bar/area), sortable paginated table, summary bar with row count and query time, Copy API URL button, CSV export. Handles loading skeletons, empty data (0 rows), and rate-limit (429) errors with distinct UI states.
- **`SavedReportCard`** — Dark card with domain badge, relative time via `date-fns`, edit/run/delete actions, and AlertDialog delete confirmation.

### Hooks

- **`use-report-builder`** — Manages `ReportDefinition` state, calls `POST /v1/reports/query` to run queries, `POST /v1/reports` and `PUT /v1/reports/:id` to save. Detects HTTP 429 and throws a `RATE_LIMIT` sentinel for UI handling.
- **`useReports`** — SWR-based CRUD hook returning `reports[]`, `isLoading`, `error`, `createReport()`, `updateReport()`, `deleteReport()`, `refetch()`. Uses optimistic updates with automatic rollback on failure.

### Metrics Registry

`reportMetrics.ts` provides a single source of truth for all available report metrics:

| Domain | Metrics |
|--------|---------|
| Indicators | CPI, GDP, Unemployment, Employment, Population, Immigration, Retail Sales, Interest Rate |
| Housing | NHPI, Building Permits, Housing Starts, Teranet HPI |
| Procurement | Contract Spending, Tender Count, Vendor Awards |
| Trade | Exports, Imports, Trade Balance |

Exports: `METRICS`, `getMetric()`, `getDefaultFilters()`, `formatValue()`, `formatDate()`, `ALL_METRICS`.

## Dependencies

Key runtime dependencies beyond Next.js/React:

| Package | Purpose |
|---------|---------|
| `recharts` ^2.12.0 | Line, bar, and area charts |
| `swr` ^2.2.5 | Data fetching with cache and optimistic updates |
| `sonner` ^1.7.4 | Toast notifications (dark theme) |
| `@radix-ui/react-alert-dialog` ^1.1.15 | Delete confirmation dialogs |
| `date-fns` ^3.6.0 | Relative time formatting |
| `nuqs` ^1.17.0 | URL query state management |
| `@candata/shared` | Shared TypeScript types and constants |
