ECHA / IUCLID data for Streamlit Cloud (GitHub + secrets)
=========================================================

IMPORTANT — DEMO / INCOMPLETE DATA
----------------------------------
What you commit under data/reach_demo/ is a **demo REACH dossier subset** for GitHub and
Streamlit Cloud size limits. It is **not** the full ECHA REACH export. You can grow the zip
as much as your repo quota allows, but **most substances will still have no dossier**, and
parsed study snippets may be **missing or partial** (heuristic parsing, not full IUCLID).
**Do not** treat this as regulatory or completeness-grade ECHA data — use official
downloads and IUCLID locally when you need authoritative coverage.

Your local folder "ECHA IUCLID database" mixes two different things:

1) IUCLID 6 format bundle (phrase / picklist decoding)
   - Local: "IUCLID 6 9.0.0_format" folder (~108 MB) or IUCLID 6 9.0.0_format.zip (~26 MB).
   - This is NOT the REACH dossier bulk; it is the IUCLID "format" package from ECHA/IUCLID downloads.
   - Recommended for GitHub: copy the extracted folder into this repo under a path with NO spaces, e.g.
       data/iuclid_format/IUCLID_6_9_0_0_format/
     (Rename when copying to avoid spaces in Cloud paths.)

2) REACH study results dossiers (substance .i6z / bulk zip)
   - Local: reach_study_results_dossiers_* is on the order of 10+ GB — too large for normal GitHub.
   - OFFLINE_LOCAL_ARCHIVE must point at a .zip, .7z, or directory of .i6z files that EXISTS in the deployed clone.
   - For Cloud demos only: create a SMALL zip (a few dossiers) as
       data/reach_demo/reach_subset.zip
     (respect GitHub file size limits).

Committed-path defaults on Streamlit Cloud
------------------------------------------
If the app detects Streamlit Cloud and you did **not** set Secrets for these variables, it will **auto-fill**
from the repo when the paths exist:

  OFFLINE_LOCAL_ARCHIVE -> <repo>/data/reach_demo/reach_subset.zip
  IUCLID_FORMAT_DIR     -> <repo>/data/iuclid_format/IUCLID_6_9_0_0_format

Secrets still override. Set HAZQUERY_DISABLE_REPO_IUCLID_DEFAULTS=1 to skip auto-fill.

Helper script (from repo root)
-------------------------------
  python scripts/prepare_iuclid_demo.py --format-src "D:\path\IUCLID 6 9.0.0_format" --i6z-dir "D:\extracted_i6z" --limit 8

Use --dry-run first. Then git add the new files under data/.

Manual PowerShell examples (adjust source paths)
------------------------------------------------
  $root = "C:\path\to\quick-hazard-assessment-app"
  New-Item -ItemType Directory -Force -Path "$root\data\iuclid_format" | Out-Null
  robocopy "C:\...\ECHA IUCLID database\IUCLID 6 9.0.0_format" "$root\data\iuclid_format\IUCLID_6_9_0_0_format" /E

Streamlit Cloud Secrets (optional if defaults exist)
------------------------------------------------------
  IUCLID_FORMAT_DIR = "/mount/src/quick-hazard-assessment-app/data/iuclid_format/IUCLID_6_9_0_0_format"
  OFFLINE_LOCAL_ARCHIVE = "/mount/src/quick-hazard-assessment-app/data/reach_demo/reach_subset.zip"

Replace "quick-hazard-assessment-app" with your GitHub repo name if different (see deploy logs).

See also: .streamlit/secrets.example.toml and README "IUCLID offline data for Streamlit Cloud".

After editing Secrets, reboot the Streamlit app.
