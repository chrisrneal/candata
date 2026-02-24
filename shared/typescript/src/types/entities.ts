/**
 * types/entities.ts — Entity graph interfaces.
 */

export interface EntityType {
  id: string;
  display_name: string;
  description: string | null;
  properties_schema: Record<string, unknown>;
}

export interface Entity {
  id: string;
  entity_type: string;
  name: string;
  external_ids: Record<string, unknown>;
  properties: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface EntityRelationship {
  id: string;
  source_entity_id: string;
  target_entity_id: string;
  relationship_type: string;
  properties: Record<string, unknown>;
  valid_from: string | null;
  valid_to: string | null;
  created_at: string;
}
