# Test CAS Numbers for ECOTOX, ToxRefDB, and CPDB

Use these CAS numbers to verify that parsed data from ECOTOX, ToxRefDB, and CPDB displays correctly in the app (indexes, columns, and associations).

## Likely in all three databases (good for full integration tests)

| CAS       | Chemical           | Notes |
|-----------|--------------------|-------|
| **71-43-2**  | Benzene            | Human carcinogen; rodent carcinogenicity (CPDB); chronic/oral studies (ToxRefDB); aquatic toxicity (ECOTOX). |
| **50-00-0**  | Formaldehyde       | Carcinogen; in CPDB, ToxRefDB, and ECOTOX. |
| **75-01-4**  | Vinyl chloride     | Carcinogen; rodent studies in all three. |
| **79-01-6**  | Trichloroethylene | Carcinogen; aquatic and chronic data. |
| **127-18-4** | Tetrachloroethylene | Carcinogen; widely tested. |
| **79-06-1**  | Acrylamide        | CPDB index confirmed; rodent carcinogen; chronic/neurotoxicity (ToxRefDB); ecotoxicity (ECOTOX). |
| **75-07-0**  | Acetaldehyde      | CPDB index confirmed; carcinogen; chronic and aquatic data. |
| **60-35-5**  | Acetamide         | CPDB index confirmed; rodent carcinogen; chronic studies. |
| **1912-24-9** | Atrazine          | Pesticide; ECOTOX (aquatic), ToxRefDB (pesticide studies), CPDB. |
| **94-75-7**  | 2,4-D             | Pesticide; in all three. |

## Confirmed in CPDB (from CPDB chemical index)

- **75-07-0** Acetaldehyde  
- **60-35-5** Acetamide  
- **103-90-2** Acetaminophen  
- **75-05-8** Acetonitrile  
- **79-06-1** Acrylamide  
- **107-02-8** Acrolein  

## How to test parsing

1. **Load the databases**  
   - Run `scripts/download_databases.py` (ToxRefDB/CPDB URLs may require manual download if 403).  
   - Place ECOTOX export in `data/raw_databases/ecotox/`.  
   - Run `python scripts/setup_chemical_db.py` to build SQLite.

2. **In the app**  
   - Query by CAS (e.g. **71-43-2**, **50-00-0**, **79-06-1**).  
   - Open expanders: **ECOTOX**, **ToxRefDB**, **CPDB**.  
   - Check: correct CAS/DTXSID, species/endpoint/values (ECOTOX), NOAEL/LOAEL/study type (ToxRefDB), TD50/tumor site/carcinogenicity (CPDB).

3. **Quick checks**  
   - Sidebar “Database coverage” shows non-zero records for ECOTOX, ToxRefDB, CPDB when data is loaded.  
   - No KeyError or missing columns; numeric columns (e.g. value_numeric, NOAEL, TD50) display when present.

## Data sources

- **CPDB:** [Summary Table by Chemical](https://cpdb.thomas-slone.org/chemicalsummary.html) (1,547 chemicals, CAS in index).  
- **ToxRefDB:** [CompTox list TOXREFDB2](https://comptox.epa.gov/dashboard/chemical-lists/TOXREFDB2) (1,176 chemicals).  
- **ECOTOX:** [EPA ECOTOX](https://cfpub.epa.gov/ecotox/) (~8,400 chemicals, CAS as identifier).
