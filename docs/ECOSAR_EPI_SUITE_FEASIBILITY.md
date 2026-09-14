# EPI Suite / ECOSAR feasibility for GHaz7 P2OASys Ecological fill

**Date:** 2026-09-14  
**Scope:** Can legacy EPA EPI Suite / ECOSAR supply acute/chronic aquatic LC50/EC50/ChV (mg/L) for Auto P2OASys Ecological Hazards, given OPERA has no aquatic endpoints?

## Verdict

**Yes — programmatically usable via the EPI Suite CLI HTTP API (`https://episuite.dev/api`), which returns ECOSAR v2.20 organics results as JSON.**  
Local Win32 `C:\EPISUITE41\Ecowinnt.exe` (ECOSAR **v1.11**) is installed and launches, but is **GUI-only** (no silent CLI). Do **not** treat OPERA as ECOSAR.

Recommended path for hazquery: thin HTTP client → parse `ecosar.modelResults` → bridge into `extra_sources` / toxicities like `opera_to_extra_sources`, then let existing `p2oasys_scorer` band LC50/EC50/ChV. Prefer measured IUCLID/SDS/ECOTOX when present; tag ECOSAR as **predicted**.

## What was found on disk

| Item | Status |
|------|--------|
| Repo wrappers / `ecosar_client` | **None** under hazquery / GHhaz* / `_github_qhaa` (only PubChem citations of EPI Suite in caches) |
| Windows install | **EPISUITE41 4.1.25** (SRC), installed 2024-06-28 |
| Install root | `C:\EPISUITE41\` |
| ECOSAR binary | `C:\EPISUITE41\Ecowinnt.exe` (PE32 x86, GUI; strings show batch dialogs → `EcoBatch.txt` / `EcoTable.txt`) |
| EPIWEB UI | `C:\EPISUITE41\EpiWeb1.exe` |
| Standalone ECOSAR 2.2 desktop | **Not installed** (EPA also notes downloadable 2.2 execution difficulties; points users to web/beta) |
| Local EPI summary files | `summary` / `tabout.txt` include fate endpoints and **ECOSAR class name only** — **not** aquatic LC50 tables |

## Programmatic run results

### Local desktop EPI / Ecowinnt

- Launching `Ecowinnt.exe` starts a GUI process (no documented argv batch).
- Community practice for silent desktop automation is fragile GUI bots (e.g. PyAutoGUI); not recommended for production.
- Batch *inside* the GUI can export tab-delimited `EcoTable.txt` (parseable), but requires interactive/UI automation.

### Remote / CLI API (succeeded)

- OpenAPI: `https://episuite.dev/api/` — **EPI Suite CLI API 1.1.0**
- Useful endpoints: `/api/search`, `/api/submit`, `/api/submit/batch` (1–100 items), `/api/download` (local JAR)
- Organic aquatic module id: **`ecosar.organics`** (not bare `ecosar`)
- Optional local runtime: `pyepisuite` ≥1.3 with downloaded JAR (`PYEPISUITE_MODE=local`) — not required for the probe below
- Unofficial Python client: [`pyepisuite`](https://pypi.org/project/pyepisuite/) (point `base_url` at `https://episuite.dev/api`; older 1.0.x default URL is stale)

**Batch probe** (`POST /api/submit/batch`, modules=`["ecosar.organics"]`), Fish / Daphnid / Green Algae only:

| Chemical | CAS | Fish 96-hr LC50 | Daphnid 48-hr LC50 | Algae 96-hr EC50 | Acute min* | Chronic ChV min* |
|----------|-----|-----------------|--------------------|------------------|------------|------------------|
| Methanol | 67-56-1 | 8092.1 | 3557.8 | 920.5 | **920.5** | **136.4** |
| Benzene | 71-43-2 | 49.04 | 28.18 | 22.06 | **22.06** | **2.84** |
| Toluene | 108-88-3 | 16.73 | 10.16 | 10.00 | **10.00** | **1.20** |
| Phenol | 108-95-2 | 30.16† | 10.24† | 37.34† | **10.24** | **1.02** |
| Naphthalene | 91-20-3 | 7.16 | 4.58 | 5.61 | **4.58** | **0.62** |

\*Min over Fish/Daphnid/Green Algae (and all returned QSAR classes for that chemical). Units **mg/L**.  
†Phenol returns two classes (e.g. Neutral Organics + Phenols); table shows the more conservative Phenols-class acute/chronic mins used for the min columns.

Raw JSON from the probe lives under:

- `docs/_ecosar_probe/panel_raw_v2.json`
- `docs/_ecosar_probe/benzene_ecosar.json`
- `docs/_ecosar_probe/panel_summary_v2.json`

### Row schema (parseable)

Each `ecosar.modelResults[]` element:

```json
{
  "qsarClass": "Neutral Organics",
  "organism": "Fish",
  "duration": "96-hr",
  "endpoint": "LC50",
  "concentration": 49.043235778808594,
  "maxLogKow": 5.0,
  "flags": []
}
```

Text `ecosar.output` also embeds an ECOSAR v2.20 table (useful for audit; prefer JSON fields for scoring).

## Parseability for P2OASys

| Need | Mapping |
|------|---------|
| Acute Aquatic | Conservative min of Fish LC50, Daphnid LC50, Green Algae EC50 (mg/L) → existing aquatic LC50 extractor / score bands |
| Chronic Aquatic | Conservative min of Fish/Daphnid/Algae **ChV** (mg/L) |
| GHS H400–H413 | Not emitted by ECOSAR; derive only if product policy already maps concentration bands → H-phrases elsewhere |
| Exclude | Earthworm, Mysid (SW), dye/polymer/surfactant modules unless structure class warrants them |

**Do not invent integer P2OASys scores in the client** — pass mg/L into `p2oasys_scorer` like other sources.

## Blockers / caveats

1. **Local Ecowinnt = no headless CLI** (GUI batch only; v1.11 ≠ API v2.20).
2. **episuite.dev is third-party hosting** of EPI/ECOSAR logic — confirm license/ToS and offline needs; use local JAR mode if air-gapped.
3. **Predicted-only** screening values; applicability domain / `flags` / `maxLogKow` must be preserved in traces.
4. Phenols and multi-class hits: take **min across classes** for conservative Eco fill (or policy-select preferred class).
5. Older `pyepisuite` defaulted to `/EpiWebSuite/api` (now SPA HTML) — use `/api`.

## Recommended next steps (smallest practical)

1. ~~**Adopt** `utils/ecosar_client.py`~~ — done (`ecosar_client_v1` + SQLite cache).
2. ~~Add `ecosar_to_extra_sources()`~~ — done (gap-fill after IUCLID/SDS).
3. ~~Wire into `p2oasys_extras_merge` / Streamlit~~ — done (`app.py`, `pages/05_P2OASys_Assessment.py`).
4. Optional: pre-batch harvest CAS into `data/ecosar_cache.sqlite`.
5. **Abandon** desktop GUI automation unless API/JAR unavailable.
6. Offline: set `HAZQUERY_SKIP_ECOSAR=1`, or point `HAZQUERY_EPISUITE_API_BASE` at a local `pyepisuite` JAR reverse-proxy.

## Related files created

- `docs/ECOSAR_EPI_SUITE_FEASIBILITY.md` (this file)
- `docs/_ecosar_probe/run_panel.py`, `run_panel_v2.py`
- `docs/_ecosar_probe/panel_*.json`, `benzene_ecosar.json`
- `utils/ecosar_client.py` (thin POC client)
