CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

CREATE TYPE data_source AS ENUM ('comptox', 'echa', 'manual');
CREATE TYPE reliability AS ENUM ('high', 'medium', 'low', 'qsar');

CREATE TABLE substances (
  substance_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  dtxsid TEXT UNIQUE,
  cas_rn TEXT,
  ec_number TEXT,
  inchikey14 TEXT,
  preferred_name TEXT NOT NULL,
  molecular_formula TEXT,
  is_uvcb BOOLEAN NOT NULL DEFAULT FALSE,
  data_sources data_source[] NOT NULL DEFAULT '{}',
  last_updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE substance_synonyms (
  id BIGSERIAL PRIMARY KEY,
  substance_id UUID NOT NULL REFERENCES substances(substance_id) ON DELETE CASCADE,
  synonym TEXT NOT NULL,
  source data_source NOT NULL
);

CREATE TABLE hazard_endpoints (
  id BIGSERIAL PRIMARY KEY,
  substance_id UUID NOT NULL REFERENCES substances(substance_id) ON DELETE CASCADE,
  source data_source NOT NULL,
  endpoint_type TEXT NOT NULL,
  hazard_code TEXT,
  result_text TEXT,
  result_value NUMERIC,
  result_unit TEXT,
  reliability reliability,
  study_date DATE,
  source_reference TEXT,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE source_versions (
  id SERIAL PRIMARY KEY,
  source data_source NOT NULL,
  version_tag TEXT NOT NULL,
  downloaded_at TIMESTAMPTZ NOT NULL,
  record_count INTEGER,
  UNIQUE (source, version_tag)
);
