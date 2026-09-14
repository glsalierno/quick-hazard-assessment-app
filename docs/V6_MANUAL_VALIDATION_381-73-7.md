# Manual validation — CAS 381-73-7 (GHhaz6)

| Field | Value |
|-------|--------|
| Date / time (UTC) | 2026-07-15T16:19:27Z |
| App URL / port | http://localhost:8502 |
| CAS assessed | 381-73-7 |
| Preferred name | 2,2-difluoroacetic acid |
| PubChem CID | 9788 |
| IUPAC / PubChem title | 2,2-difluoroacetic acid |
| Snapshot artifact | `validation_snapshots/381-73-7_v6_result_data.json` |
| Summary artifact | `validation_snapshots/381-73-7_v6_validation_summary.json` |
| How obtained | Live `ChemicalAssessmentService.assess("381-73-7")` via `scripts/run_v6_manual_validation_381.py` (same clients as Streamlit v6) |

## Retrieved H-codes

`H314`, `H227`, `H318`, `H402`, `H410`

## Displayed H-codes

`H314`, `H227`, `H402`, `H410` (each with lexicon phrases)

## Unmapped H-codes

None

## Subsumed / suppressed H-codes

| Code | Primary | Reason |
|------|---------|--------|
| H318 | H314 | H318 (serious eye damage) is often co-listed with H314; H314 already covers severe skin burns and eye damage. Phrase retained: "Causes serious eye damage." |

**Accounting check:** `retrieved == displayed + unmapped + subsumed` — **PASS** (`h_complete: true`). H318 is not silently dropped.

## Ecotoxicity endpoint table

- Quantitative ecotox endpoint rows: **0**
- Contamination phrases ("Flammable liquids", "Skin corrosion", "Serious eye damage", "Category 1", "Category 4") in endpoint rows: **none**
- Aquatic GHS codes (separate from quantitative endpoints): `H402`, `H410`
- Flag: `aquatic_ghs_classification_only = true` (aquatic GHS present; no quantitative aquatic endpoints)

## Toxicity prioritized view

- Blank endpoint+value rows in prioritized view: **0**
- Incomplete rows excluded: **0**

## Preferred name

Not blank. Resolved to **2,2-difluoroacetic acid** (aligned with PubChem title / IUPAC).

## OPERA status

- OPERA run: **ok**
- Warning (as designed):  
  `OPERA/PubChem logP disagreement; verify parser alignment or model applicability (OPERA LogP=3.98, PubChem XLogP=0.6).`  
  Disagreement flagged; results not auto-rejected.

## QA / other warnings

- Many **P-codes** are unmapped in the lexicon (e.g. P260, P264, …). They are listed as unmapped warnings, not silently dropped from accounting. Expanding the P-phrase lexicon is a follow-up, not a blocker for this H-code validation.

## Remaining concerns

1. P-code lexicon coverage is incomplete relative to PubChem returns for this CAS.
2. OPERA vs PubChem logP gap (>2) for this structure should be spot-checked against OPERA GUI / literature when using OPERA for screening.
3. Zero quantitative ecotox endpoints for this CAS means aquatic risk evidence is classification-only; that is expected here but should not be read as “no ecotoxicity.”

## UI cross-check checklist (optional)

In the Streamlit UI at http://localhost:8502 after assessing 381-73-7:

1. Open **QA / evidence integrity** — H counts and H318 subsumption reason visible.
2. Confirm disclaimer under the QA expander.
3. Optional: **Developer: QA validation snapshot** → write JSON and confirm filename.
