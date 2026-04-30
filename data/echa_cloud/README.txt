ECHA / IUCLID data for Streamlit Cloud (GitHub + secrets)
=========================================================

Your local folder "ECHA IUCLID database" mixes two different things:

1) IUCLID 6 format bundle (phrase / picklist decoding)
   - Local: "IUCLID 6 9.0.0_format" folder (~108 MB) or IUCLID 6 9.0.0_format.zip (~26 MB).
   - This is NOT the REACH dossier bulk; it is the IUCLID "format" package from ECHA/IUCLID downloads.
   - Recommended for GitHub: copy the extracted folder into this repo under a path with NO spaces, e.g.
       data/iuclid_format/IUCLID_6_9_0_0_format/
     (Rename when copying to avoid spaces in Cloud paths.)
   - Set Streamlit Secret (top-level TOML key):
       IUCLID_FORMAT_DIR = "/mount/src/quick-hazard-assessment-app/data/iuclid_format/IUCLID_6_9_0_0_format"
     Replace "quick-hazard-assessment-app" if your GitHub repo name differs (see Cloud deploy logs).

2) REACH study results dossiers (substance .i6z / bulk zip)
   - Local: reach_study_results_dossiers_* is on the order of 10+ GB — too large for normal GitHub.
   - OFFLINE_LOCAL_ARCHIVE must point at a .zip, .7z, or directory of .i6z files that EXISTS in the deployed clone.
   - For Cloud demos only: create a SMALL zip (e.g. a few dossiers) under e.g.
       data/reach_demo/reach_subset.zip
     commit it (respect GitHub file size limits), then set:
       OFFLINE_LOCAL_ARCHIVE = "/mount/src/quick-hazard-assessment-app/data/reach_demo/reach_subset.zip"

After editing Secrets, reboot the Streamlit app.
