# P2OASys golden panel

Compares automatic GHhaz6 scores (official TURI matrix) to scraped/expert references.

## Inputs

| Source | Path | Role |
|--------|------|------|
| Official matrix | `data/Hazard Matrix Group Review 9-19-23.xlsx` | Scoring |
| Ground-truth SQLite | `data/p2oasys_ground_truth.db` | Category scores (from GHhaz4 scrape) |
| Expert overall CSV | `../fastP2OASys/data/p2oasys_single_cas_clean.csv` | Overall score |

## Run

```bash
cd quick-hazard-assessment-app
python scripts/run_p2oasys_golden_panel.py --use-default-golden --limit 8
python scripts/run_p2oasys_golden_panel.py --cas 71-43-2 --cas 108-88-3
```

Outputs:

- `data/golden_panel_results.csv` — per-CAS / per-category comparison
- `data/golden_panel_summary.json` — MAE, false-green, matrix fingerprint

Primary metrics use **pure-looking** GT substance names only (product/mixture SDS scores for the same CAS are kept under `all_gt_matches` for transparency).

## Interpretation notes

- Many CAS in the scrape DB are product formulations; pure-chemical auto-scores should not be expected to match laundry-detergent expert rows.
- Process Factors / Life Cycle / Atmospheric remain sparsely populated by the automatic scorer — largest residual errors.
