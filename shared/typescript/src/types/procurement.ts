/**
 * types/procurement.ts — Federal procurement data interfaces.
 */

export interface Contract {
  id: string;
  contract_number: string | null;
  vendor_name: string;
  department: string;
  category: string | null;
  description: string | null;
  contract_value: number | null;    // CAD
  start_date: string | null;
  end_date: string | null;
  award_date: string | null;
  amendment_number: string | null;
  source_url: string | null;
  raw_data: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface Tender {
  id: string;
  tender_number: string | null;
  title: string;
  department: string;
  category: string | null;
  region: string | null;
  closing_date: string | null;
  status: string | null;
  estimated_value: number | null;
  source_url: string | null;
  raw_data: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}
