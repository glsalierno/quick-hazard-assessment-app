BEGIN;

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

CREATE UNIQUE INDEX substances_dtxsid_idx ON substances (dtxsid) WHERE dtxsid IS NOT NULL;
CREATE UNIQUE INDEX substances_cas_idx ON substances (cas_rn) WHERE cas_rn IS NOT NULL;
CREATE UNIQUE INDEX substances_ec_idx ON substances (ec_number) WHERE ec_number IS NOT NULL;
CREATE INDEX substances_inchikey_idx ON substances (inchikey14) WHERE inchikey14 IS NOT NULL;
CREATE INDEX substances_name_trgm_idx ON substances USING gin (preferred_name gin_trgm_ops);
CREATE INDEX synonyms_trgm_idx ON substance_synonyms USING gin (synonym gin_trgm_ops);
CREATE INDEX hazard_substance_source_idx ON hazard_endpoints (substance_id, source);
CREATE INDEX hazard_endpoint_type_idx ON hazard_endpoints (endpoint_type);

CREATE MATERIALIZED VIEW hazard_summary AS
SELECT
  s.substance_id,
  s.dtxsid,
  s.cas_rn,
  s.preferred_name,
  array_agg(DISTINCT h.hazard_code) FILTER (WHERE h.hazard_code IS NOT NULL) AS ghs_codes,
  bool_or(h.endpoint_type = 'pbt' AND h.result_text = 'PBT') AS is_pbt,
  max(h.inserted_at) AS last_hazard_update,
  array_agg(DISTINCT h.source) FILTER (WHERE h.source IS NOT NULL) AS contributing_sources
FROM substances s
LEFT JOIN hazard_endpoints h ON s.substance_id = h.substance_id
GROUP BY s.substance_id, s.dtxsid, s.cas_rn, s.preferred_name;

CREATE UNIQUE INDEX hazard_summary_substance_id_idx ON hazard_summary (substance_id);

COMMIT;
