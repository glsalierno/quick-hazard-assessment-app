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
