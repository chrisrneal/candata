-- =============================================================================
-- seed/cmas.sql
-- Census Metropolitan Areas (CMAs) referenced by the housing pipeline.
-- SGC codes follow Statistics Canada Standard Geographical Classification.
-- Run after provinces.sql so parent provinces exist.
-- =============================================================================

-- Look up province IDs for parent references
DO $$
DECLARE
  _id_nl  UUID; _id_ns  UUID; _id_nb  UUID; _id_qc  UUID;
  _id_on  UUID; _id_mb  UUID; _id_sk  UUID; _id_ab  UUID; _id_bc  UUID;
BEGIN
  SELECT id INTO _id_nl FROM geographies WHERE sgc_code = '10';
  SELECT id INTO _id_ns FROM geographies WHERE sgc_code = '12';
  SELECT id INTO _id_nb FROM geographies WHERE sgc_code = '13';
  SELECT id INTO _id_qc FROM geographies WHERE sgc_code = '24';
  SELECT id INTO _id_on FROM geographies WHERE sgc_code = '35';
  SELECT id INTO _id_mb FROM geographies WHERE sgc_code = '46';
  SELECT id INTO _id_sk FROM geographies WHERE sgc_code = '47';
  SELECT id INTO _id_ab FROM geographies WHERE sgc_code = '48';
  SELECT id INTO _id_bc FROM geographies WHERE sgc_code = '59';

  -- Atlantic
  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '205', 'Halifax', 'Halifax', _id_ns,
      '{"cmhc_geo_id": 580, "population_2021": 465703}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '305', 'Moncton', 'Moncton', _id_nb,
      '{"cmhc_geo_id": 260, "population_2021": 157717}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '310', 'Saint John', 'Saint John', _id_nb,
      '{"cmhc_geo_id": 360, "population_2021": 130613}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  -- Newfoundland
  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '001', 'St. John''s', 'St. John''s', _id_nl,
      '{"cmhc_geo_id": 200, "population_2021": 212579}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  -- Quebec
  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '408', 'Sherbrooke', 'Sherbrooke', _id_qc,
      '{"cmhc_geo_id": 2600, "population_2021": 227398}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '421', 'Québec', 'Québec', _id_qc,
      '{"cmhc_geo_id": 2020, "population_2021": 839311}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '433', 'Trois-Rivières', 'Trois-Rivières', _id_qc,
      '{"cmhc_geo_id": 2040, "population_2021": 161191}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '442', 'Saguenay', 'Saguenay', _id_qc,
      '{"cmhc_geo_id": 2120, "population_2021": 160980}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '462', 'Montréal', 'Montréal', _id_qc,
      '{"cmhc_geo_id": 2480, "population_2021": 4291732}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  -- Ottawa-Gatineau (cross-province; parent = Ontario)
  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '505', 'Ottawa-Gatineau', 'Ottawa-Gatineau', _id_on,
      '{"cmhc_geo_id": 1680, "population_2021": 1488307}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  -- Ontario
  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '535', 'Toronto', 'Toronto', _id_on,
      '{"cmhc_geo_id": 2270, "population_2021": 6202225}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '537', 'Hamilton', 'Hamilton', _id_on,
      '{"cmhc_geo_id": 520, "population_2021": 785184}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '541', 'Peterborough', 'Peterborough', _id_on,
      '{"cmhc_geo_id": 540, "population_2021": 125286}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '543', 'Oshawa', 'Oshawa', _id_on,
      '{"cmhc_geo_id": 1000, "population_2021": 415311}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '555', 'London', 'London', _id_on,
      '{"cmhc_geo_id": 1020, "population_2021": 543551}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '559', 'Windsor', 'Windsor', _id_on,
      '{"cmhc_geo_id": 780, "population_2021": 422630}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '568', 'Kitchener-Cambridge-Waterloo', 'Kitchener-Cambridge-Waterloo', _id_on,
      '{"cmhc_geo_id": 1140, "population_2021": 575847}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '570', 'St. Catharines-Niagara', 'St. Catharines-Niagara', _id_on,
      '{"cmhc_geo_id": 420, "population_2021": 433604}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '590', 'Brantford', 'Brantford', _id_on,
      '{"cmhc_geo_id": 480, "population_2021": 134203}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '595', 'Guelph', 'Guelph', _id_on,
      '{"cmhc_geo_id": 640, "population_2021": 165588}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '596', 'Barrie', 'Barrie', _id_on,
      '{"cmhc_geo_id": 460, "population_2021": 212856}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '620', 'Greater Sudbury', 'Grand Sudbury', _id_on,
      '{"cmhc_geo_id": 960, "population_2021": 166004}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  -- Prairies
  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '602', 'Winnipeg', 'Winnipeg', _id_mb,
      '{"cmhc_geo_id": 1900, "population_2021": 834678}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '705', 'Regina', 'Regina', _id_sk,
      '{"cmhc_geo_id": 1760, "population_2021": 249585}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '725', 'Saskatoon', 'Saskatoon', _id_sk,
      '{"cmhc_geo_id": 1780, "population_2021": 317480}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '825', 'Calgary', 'Calgary', _id_ab,
      '{"cmhc_geo_id": 140, "population_2021": 1481806}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '835', 'Edmonton', 'Edmonton', _id_ab,
      '{"cmhc_geo_id": 160, "population_2021": 1418118}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  -- British Columbia
  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '933', 'Vancouver', 'Vancouver', _id_bc,
      '{"cmhc_geo_id": 2410, "population_2021": 2642825}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '935', 'Victoria', 'Victoria', _id_bc,
      '{"cmhc_geo_id": 3340, "population_2021": 397237}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

  INSERT INTO geographies (level, sgc_code, name, name_fr, parent_id, properties) VALUES
    ('cma', '996', 'Kelowna', 'Kelowna', _id_bc,
      '{"cmhc_geo_id": 1380, "population_2021": 222162}')
  ON CONFLICT (sgc_code) DO UPDATE SET name = EXCLUDED.name, properties = EXCLUDED.properties;

END $$;
