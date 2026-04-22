"""CompTox ingestion pipeline: download, parse, upsert, and version logging."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from sqlalchemy import text
from tqdm import tqdm

from chemdb.config import SessionLocal, settings
from ingest.crosswalk import normalize_cas

COMPTOX_IDS_URL = "https://comptox.epa.gov/dashboard-api/ccdapp1/chemical-lists/download/DSSTox_Identifiers"
COMPTOX_TOXVAL_URL = "https://comptox.epa.gov/dashboard-api/ccdapp2/toxval/download"
COMPTOX_SYNONYMS_URL = "https://comptox.epa.gov/dashboard-api/ccdapp2/synonyms/download"


def _download(url: str, destination: Path) -> Path:
    headers = {"x-api-key": settings.comptox_api_key} if settings.comptox_api_key else {}
    response = requests.get(url, headers=headers, timeout=120)
    response.raise_for_status()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(response.content)
    return destination


def _standardize_toxval_units(unit: str | None) -> str | None:
    mapping = {"mg/kg-bw": "mg/kg", "mg/L": "mg/L", "ug/L": "ug/L"}
    if not unit:
        return None
    return mapping.get(unit.strip(), unit.strip())


def _upsert_substances(df: pd.DataFrame) -> int:
    required = {"DTXSID", "PREFERRED_NAME"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required CompTox columns: {missing}")

    count = 0
    with SessionLocal() as session:
        for row in tqdm(df.to_dict(orient="records"), desc="Upserting CompTox substances"):
            dtxsid = str(row.get("DTXSID", "")).strip().upper() or None
            if not dtxsid:
                continue
            cas = normalize_cas(str(row.get("CASRN", "")).strip())
            session.execute(
                text(
                    """
                    INSERT INTO substances (dtxsid, cas_rn, ec_number, inchikey14, preferred_name, molecular_formula, data_sources)
                    VALUES (:dtxsid, :cas_rn, :ec_number, :inchikey14, :preferred_name, :molecular_formula, ARRAY['comptox']::data_source[])
                    ON CONFLICT (dtxsid) DO UPDATE
                    SET cas_rn = COALESCE(EXCLUDED.cas_rn, substances.cas_rn),
                        ec_number = COALESCE(EXCLUDED.ec_number, substances.ec_number),
                        inchikey14 = COALESCE(EXCLUDED.inchikey14, substances.inchikey14),
                        preferred_name = EXCLUDED.preferred_name,
                        molecular_formula = COALESCE(EXCLUDED.molecular_formula, substances.molecular_formula),
                        data_sources = CASE
                          WHEN 'comptox' = ANY(substances.data_sources) THEN substances.data_sources
                          ELSE array_append(substances.data_sources, 'comptox'::data_source)
                        END,
                        last_updated = now()
                    """
                ),
                {
                    "dtxsid": dtxsid,
                    "cas_rn": cas,
                    "ec_number": (row.get("EC_NUMBER") or None),
                    "inchikey14": (str(row.get("INCHIKEY", "")).upper()[:14] or None),
                    "preferred_name": str(row.get("PREFERRED_NAME")).strip(),
                    "molecular_formula": (row.get("MOLECULAR_FORMULA") or None),
                },
            )
            count += 1
        session.commit()
    return count


def _load_hazards(toxval_df: pd.DataFrame) -> int:
    toxval_df = toxval_df.copy()
    toxval_df["toxval_type"] = toxval_df.get("toxval_type", pd.Series(dtype=str)).astype(str).str.lower()
    toxval_df = toxval_df[toxval_df["toxval_type"].isin({"acute_oral_ld50", "skin_sensitization", "ghs"})]

    inserted = 0
    with SessionLocal() as session:
        for row in tqdm(toxval_df.to_dict(orient="records"), desc="Loading CompTox hazards"):
            dtxsid = str(row.get("dtxsid", "")).strip().upper()
            if not dtxsid:
                continue
            sub = session.execute(text("SELECT substance_id FROM substances WHERE dtxsid = :dtxsid"), {"dtxsid": dtxsid}).scalar()
            if not sub:
                continue
            session.execute(
                text(
                    """
                    INSERT INTO hazard_endpoints
                    (substance_id, source, endpoint_type, hazard_code, result_text, result_value, result_unit, reliability, source_reference)
                    VALUES
                    (:substance_id, 'comptox', :endpoint_type, :hazard_code, :result_text, :result_value, :result_unit, :reliability, :source_reference)
                    """
                ),
                {
                    "substance_id": str(sub),
                    "endpoint_type": row.get("toxval_type"),
                    "hazard_code": row.get("hazard_code"),
                    "result_text": row.get("result_text"),
                    "result_value": row.get("toxval_numeric"),
                    "result_unit": _standardize_toxval_units(row.get("toxval_units")),
                    "reliability": (row.get("study_quality") or "medium"),
                    "source_reference": row.get("source_reference"),
                },
            )
            inserted += 1
        session.commit()
    return inserted


def _load_synonyms(df: pd.DataFrame) -> int:
    inserted = 0
    with SessionLocal() as session:
        for row in tqdm(df.to_dict(orient="records"), desc="Loading CompTox synonyms"):
            dtxsid = str(row.get("DTXSID", "")).strip().upper()
            synonym = str(row.get("SYNONYM", "")).strip()
            if not dtxsid or not synonym:
                continue
            sub = session.execute(text("SELECT substance_id FROM substances WHERE dtxsid = :dtxsid"), {"dtxsid": dtxsid}).scalar()
            if not sub:
                continue
            session.execute(
                text(
                    """
                    INSERT INTO substance_synonyms (substance_id, synonym, source)
                    VALUES (:substance_id, :synonym, 'comptox')
                    ON CONFLICT DO NOTHING
                    """
                ),
                {"substance_id": str(sub), "synonym": synonym},
            )
            inserted += 1
        session.commit()
    return inserted


def _record_version(version_tag: str, record_count: int) -> None:
    with SessionLocal() as session:
        session.execute(
            text(
                """
                INSERT INTO source_versions (source, version_tag, downloaded_at, record_count)
                VALUES ('comptox', :version_tag, :downloaded_at, :record_count)
                ON CONFLICT (source, version_tag) DO UPDATE
                SET downloaded_at = EXCLUDED.downloaded_at, record_count = EXCLUDED.record_count
                """
            ),
            {
                "version_tag": version_tag,
                "downloaded_at": datetime.now(tz=timezone.utc),
                "record_count": record_count,
            },
        )
        session.commit()


def load_comptox(ids_path: Path, toxval_path: Path, synonyms_path: Path, version_tag: str) -> None:
    ids_df = pd.read_csv(ids_path)
    toxval_df = pd.read_csv(toxval_path)
    synonyms_df = pd.read_csv(synonyms_path)

    substances_count = _upsert_substances(ids_df)
    _load_hazards(toxval_df)
    _load_synonyms(synonyms_df)
    _record_version(version_tag, substances_count)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load CompTox data into Postgres ChemDB.")
    parser.add_argument("--download", action="store_true", help="Download source files before loading.")
    parser.add_argument("--workdir", default="data/comptox", help="Local working directory for source files.")
    parser.add_argument("--version-tag", default=f"comptox_{datetime.now():%Y_%m}", help="Version tag to log.")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    workdir = Path(args.workdir)
    ids_path = workdir / "dsstox_identifiers.csv"
    toxval_path = workdir / "toxval.csv"
    synonyms_path = workdir / "synonyms.csv"

    if args.download:
        _download(COMPTOX_IDS_URL, ids_path)
        _download(COMPTOX_TOXVAL_URL, toxval_path)
        _download(COMPTOX_SYNONYMS_URL, synonyms_path)

    load_comptox(ids_path, toxval_path, synonyms_path, args.version_tag)


if __name__ == "__main__":
    main()
