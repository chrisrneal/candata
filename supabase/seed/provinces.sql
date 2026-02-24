-- =============================================================================
-- seed/provinces.sql
-- Canonical geography rows: Canada + 13 provinces/territories.
-- SGC codes follow Statistics Canada Standard Geographical Classification.
-- Run after migrations are applied.
-- =============================================================================

-- Canada (root node — no parent)
INSERT INTO geographies (id, level, sgc_code, name, name_fr, parent_id, properties)
VALUES (
  'a0000000-0000-0000-0000-000000000001',
  'country',
  '01',
  'Canada',
  'Canada',
  NULL,
  '{"iso_3166_1_alpha2": "CA", "iso_3166_1_alpha3": "CAN"}'
)
ON CONFLICT (sgc_code) DO UPDATE SET
  name    = EXCLUDED.name,
  name_fr = EXCLUDED.name_fr;

-- Provinces and Territories (parent = Canada row above)
INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties)
VALUES
  -- Provinces
  ('pr', '10', 'Newfoundland and Labrador', 'Terre-Neuve-et-Labrador',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "NL", "type": "province"}'),

  ('pr', '11', 'Prince Edward Island', 'Île-du-Prince-Édouard',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "PE", "type": "province"}'),

  ('pr', '12', 'Nova Scotia', 'Nouvelle-Écosse',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "NS", "type": "province"}'),

  ('pr', '13', 'New Brunswick', 'Nouveau-Brunswick',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "NB", "type": "province"}'),

  ('pr', '24', 'Quebec', 'Québec',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "QC", "type": "province"}'),

  ('pr', '35', 'Ontario', 'Ontario',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "ON", "type": "province"}'),

  ('pr', '46', 'Manitoba', 'Manitoba',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "MB", "type": "province"}'),

  ('pr', '47', 'Saskatchewan', 'Saskatchewan',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "SK", "type": "province"}'),

  ('pr', '48', 'Alberta', 'Alberta',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "AB", "type": "province"}'),

  ('pr', '59', 'British Columbia', 'Colombie-Britannique',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "BC", "type": "province"}'),

  -- Territories
  ('pr', '60', 'Yukon', 'Yukon',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "YT", "type": "territory"}'),

  ('pr', '61', 'Northwest Territories', 'Territoires du Nord-Ouest',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "NT", "type": "territory"}'),

  ('pr', '62', 'Nunavut', 'Nunavut',
    'a0000000-0000-0000-0000-000000000001',
    '{"abbreviation": "NU", "type": "territory"}')

ON CONFLICT (sgc_code) DO UPDATE SET
  name       = EXCLUDED.name,
  name_fr    = EXCLUDED.name_fr,
  properties = EXCLUDED.properties;
