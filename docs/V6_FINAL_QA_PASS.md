# GHhaz6 final QA pass report

**Date (UTC):** 2026-07-15  
**Scope:** Validation, small fixes, documentation cleanup, reproducible QA artifacts under `GHhaz6/quick-hazard-assessment-app` only. v5 not modified.

## Files changed this pass

| Path | Change |
|------|--------|
| `docs/V6_QA_CHANGELOG.md` | ASCII wording for H318 subsumed by H314; added validation artifact links |
| `docs/V6_GOLDEN_PANEL.md` | ASCII subsumption wording for HCl / H318 |
| `docs/V6_MANUAL_VALIDATION_381-73-7.md` | New live manual validation note |
| `docs/V6_FINAL_QA_PASS.md` | This report |
| `utils/validation_snapshot.py` | New compact JSON snapshot builder/writer |
| `app.py` | Developer expander to write validation snapshot |
| `scripts/run_v6_manual_validation_381.py` | Live validation runner for 381-73-7 |
| `tests/test_ghs_integrity.py` | Added exact 381-73-7-style regression test |
| `validation_snapshots/381-73-7_v6_result_data.json` | Live snapshot |
| `validation_snapshots/381-73-7_v6_validation_summary.json` | Live summary |

## Tests run

```text
pytest tests -q
68 passed, 1 skipped, 2 warnings
```

- **Skipped:** 1 — `tests/test_query.py` skips via `pytest.importorskip("pytest_benchmark")` when that optional plugin is absent (not introduced by this pass).
- **Warnings:** SWIG/RDKit DeprecationWarning from SDS parser import path (benign).
- **Note:** Full suite required `sqlalchemy` in the shared GHhaz5 `.venv` used to execute tests (install for collection of `test_crosswalk` / `test_query` only; no v5 source edits).

## Result for CAS 381-73-7

| Check | Result |
|-------|--------|
| Retrieved H314, H227, H318, H402, H410 | PASS |
| Each H-code displayed / unmapped / subsumed | PASS (`h_complete`) |
| H318 not silent-dropped; subsumed by H314 with reason | PASS |
| Ecotox endpoints free of classification contamination | PASS (0 endpoint rows; 0 contamination hits) |
| Aquatic GHS separate from quantitative endpoints | PASS (`H402`/`H410`; classification-only flag) |
| No blank prioritized tox rows | PASS |
| Preferred name not blank | PASS (`2,2-difluoroacetic acid`) |
| OPERA logP vs XLogP > 2 warning | PASS (OPERA 3.98 vs XLogP 0.6) |

App under validation: **http://localhost:8502**

## Remaining risks

1. Incomplete **P-code** lexicon → many unmapped P warnings (accounted, not dropped).
2. OPERA/PubChem physchem disagreement is flag-only; applicability still needs analyst judgment.
3. Classification-only aquatic evidence can be misread as “no ecotox.”
4. PubChem TOC/layout drift could still challenge ecotox section heuristics on other CAS.

## Suggested next phase

1. Expand P-phrase lexicon for commonly returned PubChem codes.
2. Golden-panel manual run for the remaining CAS list in `docs/V6_GOLDEN_PANEL.md`.
3. Optional: export-parity assertions against P2OASys payload in an automated integration test.
4. Keep v6 as the QA line; tag a v6 release once golden panel is signed off.
