# GHhaz6 QA changelog

**Scope:** Sibling copy of `GHhaz5/quick-hazard-assessment-app`.  
**Do not edit** the original v5 tree when working on v6.

## Focus areas

1. **Hazard evidence integrity** — every retrieved GHS H/P code is accounted for as displayed, unmapped, or suppressed/subsumed (with reason). No silent drop of codes that fail phrase lookup.
2. **Parser QA** — ecotoxicity extraction rejects contaminated GHS classification text; aquatic GHS codes stay separate from quantitative aquatic endpoints.
3. **Display / export parity** — QA checks compare UI GHS code sets vs CSV/JSON (and P2OASys inputs when available).
4. **OPERA alignment checks** — CSV parsed by column name; molecule ID verified; large OPERA vs PubChem logP disagreement flagged, not auto-rejected.

## Version markers

- `pubchem_client` provenance: `pubchem_client_v6`
- GHS lexicon / accounting: `ghs_formatter_v6`
- OPERA client identity checks: `opera_client_v6`
- P2OASys scorer provenance: `p2oasys_scorer_v6.2_assessment`
- P2OASys assessment schema: `p2oasys_assessment_v6.2`

## Implemented modules / hooks

| Area | Location |
|------|----------|
| Normalized evidence records | `utils/evidence_normalizer.py` |
| GHS / export QA helpers | `utils/qa_checks.py` |
| Phrase lexicon + code accounting | `utils/ghs_formatter.py` (`account_ghs_codes`, H227/H318/H402; H318 may be listed as subsumed by H314 with reason) |
| Ecotox section + endpoint guards | `utils/pubchem_client.py` |
| OPERA column-name + ID checks | `utils/opera_client.py` |
| Blank tox row filter | `utils/data_formatter.py` |
| Preferred-name fallback | `services/chemical_assessment.py` |
| QA expander + disclaimer | `app.py` |
| Regression tests | `tests/test_ghs_integrity.py`, `test_ecotox_parser.py`, `test_opera_alignment.py`, `test_toxicity_display.py` |
| Manual panel | `docs/V6_GOLDEN_PANEL.md` |
| Manual validation note (381-73-7) | `docs/V6_MANUAL_VALIDATION_381-73-7.md` |
| QA snapshot export | `utils/validation_snapshot.py` + Streamlit "Developer: QA validation snapshot" |
| Live validation helper | `scripts/run_v6_manual_validation_381.py` |

## P2OASys scoring hardening (Phase 1 — scoring trustworthiness)

Correctness fixes so automatic P2OASys scores are route-, unit-, and endpoint-valid,
and every non-null score is explainable. Scorer provenance marker: `p2oasys_scorer_v6.1_hardening`.

| Fix | Location |
|-----|----------|
| Oral LD50 requires an explicit oral route; intraperitoneal/intravenous/subcutaneous/dermal/inhalation are rejected | `utils/p2oasys_scorer.py` (`_extract_ld50_oral`) |
| Dermal LD50 requires dermal evidence — the oral→dermal fallback was removed | `utils/p2oasys_scorer.py` (`_extract_ld50_dermal`, scoring loop) |
| POD endpoints (NOAEL/LOAEL/LEL/TD50/BMD/…) are never scored as acute LD50/LC50; the ToxRefDB POD→surrogate-LD50 path was removed | `utils/p2oasys_scorer.py` |
| Inhalation LC50 scored in **ppm only**; `mg/m³` is recorded as rejected (needs molecular-weight conversion) instead of silently scored on the ppm rule | `utils/p2oasys_scorer.py` (`_extract_lc50_inhalation`) |
| Thousands/decimal comma parsing: `3,900` → 3900 (not 3.9), `3,9` → 3.9 | `utils/p2oasys_scorer.py` (`_num`, `_parse_numeric_threshold`, aquatic extractor) |
| IARC extraction is negation- and subject-aware; only explicit `Group N` counts (no bare-digit substring match). Fixes false IARC "1"/score 10 for CAS 151-21-3 | `utils/p2oasys_scorer.py` (`_extract_iarc`) |
| ToxVal records keyed on `toxval_type`; not relabeled LD50/LC50 unless the endpoint type says so | `utils/hazard_for_p2oasys.py` (`_toxval_to_toxicities`) |
| Score decision trace (evidence → normalization → matrix rule → score, plus rejected candidates); public score shape unchanged | `utils/p2oasys_scorer.py` (`compute_p2oasys_scores_with_trace`) |
| Matrix fingerprint (sha256 + kind) to pin the exact workbook used | `utils/p2oasys_scorer.py` (`matrix_fingerprint`) |
| P2OASys tab shows matrix fingerprint, a "Download scoring trace (JSON)" button, and a "Rejected evidence" expander | `app.py` |
| Regression tests for every fix above | `tests/test_p2oasys_scoring_hardening.py` |

## P2OASys assessment pipeline (Phases A → B → C → D)

Ordered delivery of automatic P2OASys improvements. Scorer provenance: `p2oasys_scorer_v6.2_assessment`. Assessment schema: `p2oasys_assessment_v6.2`.

### A — Scoring trustworthiness
| Item | Location |
|------|----------|
| Official-matrix export gate (`P2OASYS_REQUIRE_OFFICIAL_MATRIX`, checkbox to allow placeholder draft) | `config.py`, `app.py`, `p2oasys_assessment.assessment_is_exportable` |
| Qualifier / range parsing (`<`, `>`, `100-200`) with hazardous-end selection | `p2oasys_scorer.parse_measured_value` |
| mg/m³ → ppm via MW (`ppm = mg/m³ × 24.45 / MW`) | `p2oasys_scorer.mgm3_to_ppm`, `_extract_lc50_inhalation` |
| Explicit missing-data states (`No data`, `Predicted only`, category status) | `compute_p2oasys_scores_with_trace` → `missing`, `category_status` |
| KEY PHRASE matrix rows now scored (ODP/GWP / fate placeholders) | scoring loop `rtype == "phrase"` |

### B — Auditable assessment product
| Item | Location |
|------|----------|
| Source precedence tiers | `p2oasys_assessment.SOURCE_PRECEDENCE`, `prefer_evidence` |
| Versioned assessment JSON package | `p2oasys_assessment.build_assessment_package` |
| Analyst overrides with required justification | `apply_overrides` + app Step 5 |

### C — Coverage (existing capabilities)
| Item | Location |
|------|----------|
| SDS fields → extra_sources (H-codes only; P-codes omitted) | `p2oasys_source_bridges.sds_fields_to_extra_sources` |
| OPERA gap-fill (CATMoS LD50, LogBCF, VP, MolWeight) | `p2oasys_source_bridges.opera_to_extra_sources` |
| MW threaded from PubChem into hazard_data | `hazard_for_p2oasys.pubchem_to_hazard_data` |

### D — Guided review UX
| Item | Location |
|------|----------|
| Six-step P2OASys tab: identity → gather → coverage → draft → override → export | `app.py` `tab_p2o` |

### Tests
- `tests/test_p2oasys_scoring_hardening.py`
- `tests/test_p2oasys_assessment.py`

### Official matrix + golden panel (2026-07-28)

| Item | Location |
|------|----------|
| Official TURI matrix installed under `data/` | `data/Hazard Matrix Group Review 9-19-23.xlsx` |
| Scraped expert GT DB copied for local validation | `data/p2oasys_ground_truth.db` |
| Golden-panel compare harness | `scripts/run_p2oasys_golden_panel.py` |
| Formal v7 release gates | `docs/V7_RELEASE_GATES.md` |
| SDS hazard fields persisted for P2OASys | `input_handler.ChemicalInput.hazard_fields` → `st.session_state["sds_hazard_fields"]` |

## Non-goals

- Full app redesign
- Replacing PubChem/ToxVal retrieval architecture
- Automatic regulatory classification
- Mixture / concentration-aware scoring (deferred)
