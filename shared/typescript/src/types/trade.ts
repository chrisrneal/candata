/**
 * types/trade.ts — Trade flow interface.
 */

export type TradeDirection = 'import' | 'export';

export interface TradeFlow {
  id: string;
  direction: TradeDirection;
  hs_code: string;
  hs_description: string | null;
  hs_chapter: string;             // Generated: first 2 chars of hs_code
  partner_country: string;        // ISO 3166-1 alpha-3
  province: string;               // 2-digit SGC code
  ref_date: string;
  value_cad: number | null;
  volume: number | null;
  volume_unit: string | null;
  created_at: string;
  updated_at: string;
}
