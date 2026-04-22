"""Quick sample: 3 CAS via QSAR Toolbox WebSuite loader (prints DataFrame summaries)."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

# Sample CAS (common substances)
SAMPLE_CAS = """50-00-0
67-64-1
71-43-2
"""


def run_mock_demo(seed: str, cache_dir: str) -> None:
    """Show representative loader output when WebSuite is not running (fake HTTP)."""
    from ingest import qsar_toolbox_loader as qb

    mock_cache = tempfile.mkdtemp(prefix="qsar_mock_cache_")

    def make_json_resp(payload, content_type="application/json"):
        class R:
            status_code = 200
            headers = {"Content-Type": content_type}

            def raise_for_status(self):
                pass

            def json(self):
                return payload

            text = ""

        r = R()
        if isinstance(payload, str):
            r.text = payload
        return r

    def fake_get(url: str, **kwargs):
        if "about/toolbox/version" in url:
            class V:
                status_code = 200
                headers = {"Content-Type": "text/plain"}
                text = "MockToolbox/1.0"

                def raise_for_status(self):
                    pass

            return V()
        if any(x in url for x in ("/substances", "/chemicals", "/search/substances")):
            class N:
                status_code = 404
                text = "not found"

                def raise_for_status(self):
                    pass

            return N()
        return make_json_resp({})

    def fake_request(method: str, url: str, **kwargs):
        if method.upper() != "GET":
            raise NotImplementedError(method)
        if "/search/cas/" in url:
            cas_part = url.split("/search/cas/")[1].split("/")[0]
            cas_int = int(cas_part)
            names = {
                50000: ("Formaldehyde", "200-001-8", "chem-a111"),
                67641: ("Acetone", "200-662-2", "chem-b222"),
                71432: ("Acrylonitrile", "203-466-5", "chem-c333"),
            }.get(cas_int, ("Unknown", None, "chem-unknown"))
            name, ec, cid = names
            return make_json_resp(
                [{"ChemId": cid, "ECNumber": ec, "Cas": cas_int, "Names": [name], "SubstanceType": "Mock"}]
            )
        if "/data/all/" in url:
            return make_json_resp(
                {
                    "Physical Chemical Properties": {"Vapour pressure": {"Value": 123.4, "Unit": "mmHg"}},
                    "Human Health": {
                        "GHS": {
                            "Classification": {
                                "HazardStatements": [
                                    {"code": "H302", "text": "Harmful if swallowed."},
                                    {"code": "H315", "text": "Causes skin irritation."},
                                ]
                            }
                        },
                    },
                }
            )
        return make_json_resp({})

    print("\n======== MOCK RUN (no WebSuite) ========")
    print("Simulated JSON shapes like GET /search/cas/... and GET /data/all/{ChemId}.\n")

    session = qb.create_qsar_session()
    base = qb._session_base_url(session, None)

    with patch.object(session, "get", side_effect=fake_get), patch.object(session, "request", side_effect=fake_request):
        reg = qb.get_all_substances(session, page_size=10, cache_dir=mock_cache, base_url=base)
        cl = qb.get_all_classifications(session, reg, page_size=10, cache_dir=mock_cache, base_url=base)
        reg2, cl2 = qb.to_echa_loader_frames(reg, cl)

    print("--- registered substances ---")
    print(reg.to_string(index=False))
    print("\n--- raw classification rows (first 12) ---")
    print(cl.head(12).to_string(index=False) if len(cl) else "(empty)")
    print("\n--- echa_loader-shaped (registered) ---")
    print(reg2.to_string(index=False))
    print("\n--- echa_loader-shaped (C&L / hazards, first 12) ---")
    print(cl2.head(12).to_string(index=False) if len(cl2) else "(empty)")
    Path(seed).unlink(missing_ok=True)


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo))
    os.chdir(repo)
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
        f.write(SAMPLE_CAS)
        seed = f.name
    os.environ["QSAR_SUBSTANCE_SEED_PATH"] = seed
    os.environ.setdefault("QSAR_CACHE_DIR", str(repo / "data" / "qsar_cache_sample"))
    # Match Toolbox Server "Listening on localhost:####" (e.g. 8804); override via env.
    os.environ.setdefault("QSAR_TOOLBOX_PORT", os.environ.get("QSAR_TOOLBOX_PORT", "8804"))

    from ingest import qsar_toolbox_loader as qb

    print("=== QSAR Toolbox sample (3 CAS) ===")
    print(f"Seed file: {seed}")
    print(f"Port: {os.environ.get('QSAR_TOOLBOX_PORT')}")
    print(f"Cache: {os.environ.get('QSAR_CACHE_DIR')}")
    print()

    try:
        session = qb.create_qsar_session()
        base = qb._session_base_url(session, None)
        ver = qb.assert_websuite_alive(session, base)
        print(f"Toolbox WebAPI OK — version: {ver!r}")
        print(f"API base: {base}")
    except Exception as exc:
        print(f"Toolbox WebAPI not available: {exc}")
        print("(Start Toolbox Server / WebSuite; for BadStatusLine over HTTP, use QSAR_SCHEME=auto or https — see .env.example.)\n")
        run_mock_demo(seed, os.environ["QSAR_CACHE_DIR"])
        return

    reg = qb.get_all_substances(session, page_size=10, cache_dir=os.environ["QSAR_CACHE_DIR"], base_url=base)
    print("\n--- registered substances (sample) ---")
    print(reg.to_string())
    cl = qb.get_all_classifications(session, reg, page_size=10, cache_dir=os.environ["QSAR_CACHE_DIR"], base_url=base)
    print("\n--- classifications / hazard rows (sample) ---")
    print(cl.head(20).to_string() if len(cl) else "(no rows extracted)")
    reg2, cl2 = qb.to_echa_loader_frames(reg, cl)
    print("\n--- after to_echa_loader_frames (columns for echa_loader) ---")
    print("registered columns:", list(reg2.columns))
    print(reg2.to_string())
    print("cl columns:", list(cl2.columns))
    print(cl2.head(15).to_string() if len(cl2) else "(empty)")
    Path(seed).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
