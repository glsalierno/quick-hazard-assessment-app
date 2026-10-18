# CAS Common Chemistry retrieval capability test

**Scope:** GHhaz6 isolated retrieval test; no Streamlit integration.  
**Use:** Academic/noncommercial research under CAS Common Chemistry CC BY-NC 4.0.  
**Live run:** 2026-07-16 UTC  
**Parser:** `cas_commonchem_client_v6_1`

## Result

The authenticated CAS Common Chemistry API was tested through `/search`,
`/detail`, and `/export`.

| Measure | Result |
|---|---:|
| Default-panel chemicals | 9 |
| Detail records found | 8 |
| Records with experimental properties | 8 |
| Experimental property rows | 23 |
| Distinct property types | 3 |
| Records with molfiles | 8 |
| Successful molfile export probe | Yes |
| Structured toxicity-like fields | 0 |
| Toxicity-like experimental property names | 0 |

The physical-property types observed were:

- Boiling Point
- Melting Point
- Density

CAS Common Chemistry returned identity and structure fields, synonyms,
replaced CAS RNs, property citations, and molfile availability. The client
preserves the original property text and performs best-effort extraction of
the primary numeric value/range, unit, and measurement conditions.

## Per-chemical coverage

| CAS RN | CAS name | Property rows | Outcome |
|---|---|---:|---|
| 381-73-7 | Difluoroacetic acid | 3 | Found |
| 67-56-1 | Methanol | 3 | Found |
| 71-43-2 | Benzene | 3 | Found |
| 7732-18-5 | Water | 3 | Found |
| 7647-01-0 | Hydrochloric acid | 3 | Found |
| 50-00-0 | Formaldehyde | 3 | Found |
| 108-88-3 | Toluene | 3 | Found |
| 1607-31-4 | - | 0 | Search returned zero; detail returned HTTP 404 |
| 1912-24-9 | Atrazine | 2 | Found |

## Interpretation

CAS Common Chemistry is valuable to GHhaz6 as:

1. A curated CAS identity and preferred-name source.
2. A source of canonical structure identifiers and molfiles.
3. A source of selected experimental physical properties with citations.
4. A source of replaced/deleted CAS RN evidence.
5. A cross-source QA comparator for PubChem and OPERA.

It is **not** a structured toxicity endpoint source in the tested API
responses. Toxicity retrieval should remain with ToxValDB, PubChem, IUCLID,
CPDB/IARC, and clearly identified QSAR sources. A citation mentioning HSDB
does not turn the CAS physical-property row into a toxicity endpoint.

## Reproducible artifacts

- Client: `utils/cas_commonchem_client.py`
- Unit tests: `tests/test_cas_commonchem_client.py`
- Live probe: `scripts/probe_cas_commonchem_capabilities.py`
- Sanitized JSON: `validation_snapshots/cas_commonchem/default_panel_capability.json`
- Summary: `validation_snapshots/cas_commonchem/default_panel_capability.md`

Run:

```powershell
python scripts/probe_cas_commonchem_capabilities.py
```

The key is loaded from `CAS_COMMONCHEM_API_KEY` or the gitignored
`.streamlit/secrets.toml`. Artifacts omit the key, SVG images, complete raw
responses, and molfile content.

## Verification

```text
CAS client unit tests: 6 passed
Complete GHhaz6 suite: 74 passed, 1 skipped, 2 warnings
Skipped: optional pytest_benchmark dependency unavailable
Warnings: existing SWIG deprecation warnings in the SDS parser test
Artifact secret/structure-content scan: no matches
```

## Streamlit integration status

Implemented in GHhaz6:

1. CAS detail retrieval is optional, fail-soft, and cached by CAS RN for 24 hours.
2. A dedicated **CAS Common Chemistry** Streamlit tab shows every returned
   experimental property generically, plus identity/structure fields,
   citations, synonyms, replaced RNs, provenance, field coverage, and a
   normalized JSON download.
3. OPERA runs automatically when PubChem supplies a valid SMILES.
4. A source-labeled CAS/PubChem/OPERA comparison table shows experimental,
   retrieved, and predicted values without silently choosing a winner.
5. Large LogP, melting-point, boiling-point, density, and molecular-weight
   disagreements produce non-blocking QA warnings.
6. CAS physical-property evidence remains separate from toxicity endpoints and
   GHS classifications.

Comparison implementation: `utils/property_comparison.py`  
Comparison tests: `tests/test_property_comparison.py`
