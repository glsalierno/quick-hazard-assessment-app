# V7 release gates — Quick Hazard Assessment / automatic P2OASys

**Policy:** Keep developing on **v6**. The name **v7** is earned only when the gates below are met. Until then, every assessment is a *draft for expert review*.

## Gate 0 — Official matrix (required)

- [ ] `data/Hazard Matrix Group Review 9-19-23.xlsx` is present (or `P2OASYS_MATRIX_PATH` points to it).
- [ ] `resolve_p2oasys_matrix_path` returns `kind == "official"`.
- [ ] Assessment export does not require the placeholder checkbox.
- [ ] Assessment package records matrix `sha256` + filename.

**Status (2026-07-28):** Official matrix installed in GHhaz6 `data/`; fingerprint
`ab0ea28a8b4da024…` recorded in `data/golden_panel_summary.json`.

## Gate 1 — Scoring trustworthiness

- [x] Route-valid oral/dermal LD50 (no oral→dermal fallback; no ip/iv as oral).
- [x] POD endpoints (NOAEL/LOAEL/…) never scored as acute LD50.
- [x] Inhalation: ppm preferred; mg/m³ converted with MW when available.
- [x] Thousands separators and qualifiers/`ranges` handled transparently.
- [x] IARC negation / subject awareness.
- [x] Regression unit tests for the above (`tests/test_p2oasys_scoring_hardening.py`).

## Gate 2 — Auditable assessment product

- [x] Score decision traces (evidence → rule → score + rejected/missing).
- [x] Versioned assessment JSON package (`p2oasys_assessment_v6.2`).
- [x] Analyst overrides require justification.
- [x] HTML executive summary from the same package object.
- [ ] Multi-source conflict reconciliation populated (not just empty `conflicts: []`).

## Gate 3 — Coverage (SDS / OPERA / lookups)

- [x] OPERA gap-fill bridge (CATMoS / LogBCF / VP / MW) with predicted tags.
- [x] SDS hazard fields persisted to session and merged into P2OASys extras.
- [ ] SDS bridge verified end-to-end on a real SDS PDF in CI/manual QA.
- [ ] Fate / atmospheric / Process Factors rules produce calibrated scores on the official matrix (Process Factors currently drives most false-greens).

## Gate 4 — Golden-panel performance

Run:

```bash
python scripts/run_p2oasys_golden_panel.py --use-default-golden --limit 10
```

Reference sources:

- `data/p2oasys_ground_truth.db` (category scores; from GHhaz4 scrape corpus)
- `fastP2OASys/data/p2oasys_single_cas_clean.csv` (overall expert scores)

**First panel (8 curated CAS, 2026-07-28, pure-GT metrics):**

| Metric | Result | Proposed gate |
|--------|--------|----------------|
| Category MAE | **1.5** | ≤ 2.0 |
| False-green rate | **8.3%** | ≤ 10% |
| Category pairs | 24 | — |
| Overall MAE vs expert CSV | 2.1 | (informational) |

Remaining false-greens are concentrated in **Process Factors** (auto≈2 vs expert 8–9 for benzene/toluene).

- [x] First documented panel run archived (`data/golden_panel_summary.json`).
- [ ] Thresholds formally ratified; larger pure-CAS panel (≥50) re-run.
- [ ] Panel re-run in CI or pre-release checklist.

## Gate 5 — UX / reporting

- [x] Guided P2OASys tab (identity → gather → coverage → draft → override → export).
- [x] Assessment JSON + HTML download.
- [ ] PDF executive summary (optional).
- [ ] PubChem-independent assessment path (local DSSTox/ToxVal/SDS can drive scores without requiring PubChem success).

## Gate 6 — Engineering hygiene

- [ ] `streamlit` / `SQLAlchemy` declared so the documented test suite is reproducible.
- [x] Golden-panel unit loaders + hardening tests green (`43` related tests).
- [ ] Changelog / VERSION bump announcing v7.

## How to declare v7

When Gates 0–6 are checked:

1. Tag release `v7.0.0`.
2. Set app subtitle / README to “v7 — draft P2OASys for expert review (TURI matrix calibrated)”.
3. Archive the golden-panel summary JSON with the release notes.
