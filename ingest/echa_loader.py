"""ECHA ingestion pipeline: registered substances + C&L hazards."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

import pandas as pd
import requests
from sqlalchemy import text
from tqdm import tqdm

from chemdb.config import SessionLocal
from ingest.crosswalk import is_uvcb_name, normalize_cas

ECHA_REGISTERED_URL = "https://echa.europa.eu/documents/10162/13634/registered_substances.csv"
ECHA_CL_URL = "https://echa.europa.eu/documents/10162/23036412/cl_inventory.csv"


def _download(url: str, destination: Path) -> Path:
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(response.content)
    return destination


def _find_substance_id(session, ec_number: str | None, cas_rn: str | None):
    if ec_number:
        row = session.execute(text("SELECT substance_id FROM substances WHERE ec_number = :ec"), {"ec": ec_number}).scalar()
        if row:
            return row
    if cas_rn:
        return session.execute(text("SELECT substance_id FROM substances WHERE cas_rn = :cas"), {"cas": cas_rn}).scalar()
    return None


def _upsert_substances(df: pd.DataFrame) -> int:
    count = 0
    with SessionLocal() as session:
        for row in tqdm(df.to_dict(orient="records"), desc="Upserting ECHA substances"):
            name = str(row.get("substance_name", "")).strip()
            ec = str(row.get("ec_number", "")).strip() or None
            cas = normalize_cas(str(row.get("cas_rn", "")).strip())
            is_uvcb = is_uvcb_name(name)
            if is_uvcb and not (ec or cas):
                continue

            existing_id = _find_substance_id(session, ec, cas)
            if existing_id:
                session.execute(
                    text(
                        """
                        UPDATE substances
                        SET preferred_name = COALESCE(NULLIF(:preferred_name, ''), preferred_name),
                            cas_rn = COALESCE(:cas_rn, cas_rn),
                            ec_number = COALESCE(:ec_number, ec_number),
                            is_uvcb = is_uvcb OR :is_uvcb,
                            data_sources = CASE
                              WHEN 'echa' = ANY(data_sources) THEN data_sources
                              ELSE array_append(data_sources, 'echa'::data_source)
                            END,
                            last_updated = now()
                        WHERE substance_id = :substance_id
                        """
                    ),
                    {
                        "substance_id": str(existing_id),
                        "preferred_name": name,
                        "cas_rn": cas,
                        "ec_number": ec,
                        "is_uvcb": is_uvcb,
                    },
                )
            else:
                session.execute(
                    text(
                        """
                        INSERT INTO substances (cas_rn, ec_number, preferred_name, is_uvcb, data_sources)
                        VALUES (:cas_rn, :ec_number, :preferred_name, :is_uvcb, ARRAY['echa']::data_source[])
                        """
                    ),
                    {"cas_rn": cas, "ec_number": ec, "preferred_name": name, "is_uvcb": is_uvcb},
                )
            count += 1
        session.commit()
    return count


def _load_cl_hazards(df: pd.DataFrame) -> int:
    inserted = 0
    with SessionLocal() as session:
        for row in tqdm(df.to_dict(orient="records"), desc="Loading ECHA C&L hazards"):
            ec = str(row.get("ec_number", "")).strip() or None
            cas = normalize_cas(str(row.get("cas_rn", "")).strip())
            hazard_code = str(row.get("hazard_code", "")).strip() or None
            classification = str(row.get("classification", "")).strip() or None
            endpoint_type = "ghs_classification" if hazard_code else "classification_note"
            if not (ec or cas):
                continue

            substance_id = _find_substance_id(session, ec, cas)
            if not substance_id:
                continue

            session.execute(
                text(
                    """
                    INSERT INTO hazard_endpoints
                    (substance_id, source, endpoint_type, hazard_code, result_text, reliability, source_reference)
                    VALUES
                    (:substance_id, 'echa', :endpoint_type, :hazard_code, :result_text, :reliability, :source_reference)
                    """
                ),
                {
                    "substance_id": str(substance_id),
                    "endpoint_type": endpoint_type,
                    "hazard_code": hazard_code,
                    "result_text": classification,
                    "reliability": "high",
                    "source_reference": row.get("source_reference") or "ECHA C&L Inventory",
                },
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
                VALUES ('echa', :version_tag, :downloaded_at, :record_count)
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


def load_echa(registered_path: Path, cl_path: Path, version_tag: str) -> None:
    registered_df = pd.read_csv(registered_path)
    cl_df = pd.read_csv(cl_path)
    count = _upsert_substances(registered_df)
    _load_cl_hazards(cl_df)
    _record_version(version_tag, count)


def load_echa_from_qsar_toolbox(version_tag: str) -> None:
    """Load identifiers + endpoint-derived hazard text via local QSAR Toolbox WebSuite (``/api/v6``)."""
    from ingest import qsar_toolbox_loader as qb

    reg, cl = qb.load_echa_from_qsar_toolbox()
    reg2, cl2 = qb.to_echa_loader_frames(reg, cl)
    count = _upsert_substances(reg2)
    inserted = _load_cl_hazards(cl2)
    _record_version(version_tag, count)
    logger.info("QSAR Toolbox WebSuite load complete: substances=%s hazard_rows=%s", count, inserted)


def load_echa_from_iuclid_api(version_tag: str) -> None:
    """Load REACH-style identifiers + CLP/GHS signals via IUCLID Public REST API (see ``iuclid_api_loader``)."""
    from ingest.iuclid_api_loader import registered_and_cl_dataframes, to_echa_loader_frames

    reg, cl = registered_and_cl_dataframes()
    reg2, cl2 = to_echa_loader_frames(reg, cl)
    count = _upsert_substances(reg2)
    inserted = _load_cl_hazards(cl2)
    _record_version(version_tag, count)
    logger.info("IUCLID API load complete: substances=%s hazard_rows=%s", count, inserted)


def load_echa_from_offline(
    version_tag: str,
    *,
    force_download: bool = False,
    force_rebuild: bool = False,
    max_substances_for_cl: int | None = None,
) -> None:
    """Bulk IUCLID REACH dossiers + targeted ECHA CHEM scrape (see ``offline_echa_loader``)."""
    from ingest import offline_echa_loader as ob
    from ingest.qsar_toolbox_loader import to_echa_loader_frames

    reg, cl = ob.load_echa_from_offline(
        use_cache=True,
        force_rebuild=force_rebuild,
        force_download=force_download,
        max_substances_for_cl=max_substances_for_cl,
    )
    reg2, cl2 = to_echa_loader_frames(reg, cl)
    count = _upsert_substances(reg2)
    inserted = _load_cl_hazards(cl2)
    _record_version(version_tag, count)
    logger.info("Offline ECHA load complete: substances=%s hazard_rows=%s", count, inserted)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load ECHA data into Postgres ChemDB.")
    parser.add_argument("--download", action="store_true", help="Download source files before loading.")
    parser.add_argument(
        "--use-qsar-toolbox",
        action="store_true",
        help="Use local QSAR Toolbox WebSuite WebAPI (or set USE_QSAR_TOOLBOX=true). Takes precedence over IUCLID.",
    )
    parser.add_argument(
        "--use-iuclid-api",
        action="store_true",
        help="Use IUCLID Public REST API instead of static CSV downloads (or set USE_IUCLID_API=true).",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use offline REACH bulk + ECHA CHEM scrape (or set USE_OFFLINE_ECHA=true). After QSAR/IUCLID in priority.",
    )
    parser.add_argument(
        "--offline-force-download",
        action="store_true",
        help="Re-download REACH archive even if present under OFFLINE_DATA_DIR.",
    )
    parser.add_argument(
        "--offline-max-cl",
        type=int,
        default=None,
        metavar="N",
        help="Scrape C&L from ECHA CHEM for at most N substances (testing / partial runs).",
    )
    parser.add_argument("--workdir", default="data/echa", help="Local working directory for source files.")
    parser.add_argument("--version-tag", default=f"echa_{datetime.now():%Y_q%m}", help="Version tag to log.")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    use_qsar = args.use_qsar_toolbox or os.getenv("USE_QSAR_TOOLBOX", "").strip().lower() in ("1", "true", "yes", "on")
    use_iuclid = args.use_iuclid_api or os.getenv("USE_IUCLID_API", "").strip().lower() in ("1", "true", "yes", "on")
    use_offline = args.offline or os.getenv("USE_OFFLINE_ECHA", "").strip().lower() in ("1", "true", "yes", "on")
    if use_qsar:
        load_echa_from_qsar_toolbox(args.version_tag)
        return
    if use_iuclid:
        load_echa_from_iuclid_api(args.version_tag)
        return
    if use_offline:
        max_cl = args.offline_max_cl
        if max_cl is None and os.getenv("OFFLINE_MAX_CL", "").strip().isdigit():
            max_cl = int(os.getenv("OFFLINE_MAX_CL", "").strip())
        load_echa_from_offline(
            args.version_tag,
            force_download=args.offline_force_download,
            force_rebuild=args.offline_force_download,
            max_substances_for_cl=max_cl,
        )
        return

    workdir = Path(args.workdir)
    registered_path = workdir / "registered_substances.csv"
    cl_path = workdir / "cl_inventory.csv"
    if args.download:
        _download(ECHA_REGISTERED_URL, registered_path)
        _download(ECHA_CL_URL, cl_path)
    load_echa(registered_path, cl_path, args.version_tag)


if __name__ == "__main__":
    main()
