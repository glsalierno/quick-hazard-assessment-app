# Local data (optional, not committed)

Place large third-party files here while testing the **public** build, for example:

- `reach_study_results_dossiers_*.zip`
- `reach_study_results-dossier_info_*.xlsx`
- Extracted IUCLID format folder (or keep it elsewhere)

Then set environment variables to **absolute paths** (recommended on Windows), e.g.:

```powershell
$env:OFFLINE_LOCAL_ARCHIVE = (Resolve-Path ".\local_data\reach_study_results_dossiers_23-05-2023.zip").Path
$env:IUCLID_FORMAT_DIR = (Resolve-Path ".\local_data\IUCLID6_6_format_9.0.0").Path
```

Do not commit proprietary dossiers or licensed IUCLID bundles unless your repository policy allows it.
