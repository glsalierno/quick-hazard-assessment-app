# REACH demo dossiers (`reach_subset.zip`)

This folder holds a **small zip of `.i6z` REACH study-result dossiers** intended for **demos**, Streamlit Cloud, and developer tests. It is **not** the official full ECHA REACH bulk export.

## Expectations

- **Coverage:** Only substances present in the zip can appear in the offline index. **Most CAS numbers will not match** unless you expand the archive (still bounded by GitHub / hosting limits).
- **Completeness:** Even for an included substance, extracted tables and snippets may be **incomplete**; the app uses **heuristic** parsing of IUCLID XML, not the full IUCLID application.
- **Use:** **Not** for regulatory submissions, registration decisions, or claims about REACH completeness. For authoritative data, use [ECHA IUCLID 6 downloads](https://iuclid6.echa.europa.eu/downloads) and a **local** full `reach_study_results_dossiers_*.zip` (or folder of `.i6z` files).

## Populate

From the repo root:

```bash
python scripts/prepare_iuclid_demo.py --format-src "PATH/TO/IUCLID 6 9.0.0_format" --i6z-dir "PATH/TO/FOLDER_WITH_I6Z" --limit 20
```

Increase `--limit` until you approach comfortable repo size. Then `git add data/reach_demo/reach_subset.zip` (and the format tree under `data/iuclid_format/` if needed).

The Streamlit **REACH / IUCLID** expander shows an on-screen reminder when this committed demo path is in use.
