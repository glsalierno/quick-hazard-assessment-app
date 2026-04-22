"""Validate offline ECHA loader without requiring the multi-GB REACH archive.

Runs:
1. **XML unit check** — ``_parse_i6d_xml`` on a tiny synthetic ``Document.i6d``-like snippet.
2. **Optional archive** — if ``OFFLINE_TEST_ARCHIVE`` points to a ``.7z`` or ``.zip`` that already contains ``.i6z`` files, runs ``extract_i6z_metadata`` (can be slow).
3. **Optional CHEM smoke** — if ``OFFLINE_CHEM_SMOKE=1``, one ``fetch_cl_from_echa_chem`` call (polite delay; needs network).

Manual disclaimer capture (IUCLID bulk):
- Open ``REACH_STUDY_INDEX_URL`` (default IUCLID REACH Study Results) in a browser, accept the legal terms.
- In **Developer tools → Network**, find the **POST** immediately before the ``.7z`` **GET** (often a ``terms`` / ``accept`` URL).
- Copy **Request URL** into ``DISCLAIMER_URL`` or ``IUCLID_ACCEPT_POST_URL`` and **form body** (or JSON) into ``DISCLAIMER_POST_DATA`` / ``IUCLID_ACCEPT_POST_BODY`` in ``.env``.
- Set ``REACH_STUDY_RESULTS_URL`` to the exact ``.7z`` link if scraping the index page fails.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)


def main() -> None:
    from ingest import offline_echa_loader as ob

    sample = b"""<?xml version="1.0" encoding="UTF-8"?>
    <Root xmlns="http://iuclid6.echa.europa.eu/namespaces/platform-document/v1">
      <SubstanceName>Benzene</SubstanceName>
      <ECNumber>200-753-7</ECNumber>
      <CASNumber>71-43-2</CASNumber>
      <RegistrationDate>2010-01-01</RegistrationDate>
      <RegulatoryPool>REACH</RegulatoryPool>
      <GHSClassification><HazardStatementCode>H314</HazardStatementCode></GHSClassification>
    </Root>
    """
    meta, ghs_sample = ob._parse_i6d_xml(sample)
    print("_parse_i6d_xml:", meta, "ghs_rows:", len(ghs_sample))
    assert meta.get("substance_name") == "Benzene"
    assert "71" in str(meta.get("cas_number") or "")
    assert any((r.get("h_statement_code") == "H314") for r in ghs_sample)

    arch = os.getenv("OFFLINE_TEST_ARCHIVE", "").strip()
    if arch:
        df, cl_i6 = ob.extract_i6z_metadata(
            Path(arch),
            Path(os.getenv("OFFLINE_TEST_EXTRACT_DIR", "data/offline/test_extract")),
            force_extract=True,
        )
        print("extract_i6z_metadata substances shape:", df.shape, "i6d CL rows:", len(cl_i6))
        print(df.head(3))
    else:
        print("Skip archive test (set OFFLINE_TEST_ARCHIVE to a .zip or .7z path to run).")

    if os.getenv("OFFLINE_CHEM_SMOKE", "").strip().lower() in ("1", "true", "yes"):
        s = ob.create_offline_session()
        rows = ob.fetch_cl_from_echa_chem(
            s,
            "200-753-7",
            "71-43-2",
            "benzene",
            Path(os.getenv("OFFLINE_CACHE_DIR", "data/offline_cache")) / "chem_smoke",
            min_delay_s=1.5,
        )
        print("CHEM smoke rows:", len(rows))
        print(rows[:3])
    else:
        print("Skip CHEM smoke (set OFFLINE_CHEM_SMOKE=1 to hit chem.echa.europa.eu).")

    print("OK")


if __name__ == "__main__":
    main()
