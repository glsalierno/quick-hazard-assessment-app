# data/

Local SQLite used by the Streamlit app.

| File | Role |
|------|------|
| `cameo_nfpa.sqlite` | Slim CAMEO Chemicals 3.1.0 NFPA 704 extract (one preferred non-mixture row per CAS). Commit this. |
| `chemical_db.sqlite` | Built locally by `scripts/setup_chemical_db.py` (DSSTox + optional ToxValDB). Do not replace CAMEO with this file. |

Optional: install [CAMEO Chemicals 3.1.0](https://www.epa.gov/cameo/cameo-chemicals-software) or set `CAMEO_SQLITE` to use the full desktop database. Lookups never scrape cameochemicals.noaa.gov.
