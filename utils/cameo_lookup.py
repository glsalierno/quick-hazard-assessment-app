"""
CAMEO Chemicals NFPA 704 lookup (read-only SQLite).

Prefers the NOAA/EPA CAMEO Chemicals 3.1.0 desktop database when installed.
Falls back to a slim bundled ``cameo_nfpa.sqlite`` (one preferred row per CAS).
Does not scrape cameochemicals.noaa.gov.

When several CAMEO records share a CAS (pure substance vs mixture), prefer the
non-mixture primary record. Never inherit a mixture diamond (e.g. DCM/MeCl
Flame 4, CO2/O2 Health 3) onto the solvent CAS.
"""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Optional

DEFAULT_CAMEO_SQLITE = Path(
    r"C:\Program Files (x86)\CAMEO Chemicals 3.1.0"
    r"\resources\server\CAMEOChemicalsServer\_internal\cameo.sqlite"
)
ALT_CAMEO_SQLITE = Path(
    r"C:\Program Files\CAMEO Chemicals 3.1.0"
    r"\resources\server\CAMEOChemicalsServer\_internal\cameo.sqlite"
)

DATASHEET_URL = "https://cameochemicals.noaa.gov/chemical/{id}"
BUNDLE_NAME = "cameo_nfpa.sqlite"

_MIXTURE_RE = re.compile(
    r"\bMIXTURES?\b|\bSOLUTIONS?\b|\bWITH MORE THAN\b|\bWITH NOT MORE THAN\b",
    re.I,
)


def normalize_cas(cas: str | None) -> str:
    if not cas:
        return ""
    return "".join(c for c in str(cas) if c.isdigit())


def format_cas_display(cas: str | None) -> str:
    digits = normalize_cas(cas)
    if not digits:
        return (cas or "").strip()
    if len(digits) >= 5:
        return f"{digits[:-3]}-{digits[-3:-1]}-{digits[-1]}"
    return digits


def _bundled_candidates() -> list[Path]:
    here = Path(__file__).resolve().parent
    out: list[Path] = []
    for p in (
        here / "data" / BUNDLE_NAME,
        here.parent / "data" / BUNDLE_NAME,
        here.parents[1] / "data" / BUNDLE_NAME if len(here.parents) > 1 else None,
        here.parents[2] / "data" / BUNDLE_NAME if len(here.parents) > 2 else None,
        here / BUNDLE_NAME,
    ):
        if p is not None:
            out.append(p)
    return out


def resolve_cameo_sqlite(explicit: Path | None = None) -> Optional[Path]:
    env = (os.environ.get("CAMEO_SQLITE") or "").strip()
    ranked: list[Path] = []
    if explicit:
        ranked.append(Path(explicit))
    if env:
        ranked.append(Path(env))
    ranked.extend((DEFAULT_CAMEO_SQLITE, ALT_CAMEO_SQLITE))
    ranked.extend(_bundled_candidates())
    seen: set[Path] = set()
    for p in ranked:
        try:
            rp = p.resolve()
        except OSError:
            continue
        if rp in seen:
            continue
        seen.add(rp)
        if rp.is_file() and rp.stat().st_size > 1000:
            return rp
    return None


def _connect(path: Path) -> sqlite3.Connection:
    uri = path.resolve().as_posix()
    if not uri.startswith("/"):
        uri = "/" + uri
    con = sqlite3.connect(f"file:{uri}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _table_names(con: sqlite3.Connection) -> set[str]:
    rows = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {str(r[0]) for r in rows}


def _is_full_schema(tables: set[str]) -> bool:
    return "chemicals" in tables and "chemical_cas" in tables


def cameo_status(explicit: Path | None = None) -> dict[str, Any]:
    path = resolve_cameo_sqlite(explicit)
    out: dict[str, Any] = {
        "available": False,
        "path": str(path) if path else None,
        "version": None,
        "schema": None,
        "n_chemicals": 0,
        "n_nfpa": 0,
    }
    if not path:
        return out
    try:
        con = _connect(path)
        tables = _table_names(con)
        if _is_full_schema(tables):
            info = con.execute("SELECT version FROM info").fetchone()
            n_chem = con.execute("SELECT COUNT(*) FROM chemicals").fetchone()[0]
            n_nfpa = con.execute(
                "SELECT COUNT(*) FROM chemicals "
                "WHERE nfpa_health IS NOT NULL OR nfpa_flam IS NOT NULL"
            ).fetchone()[0]
            out.update(
                available=True,
                version=info[0] if info else None,
                schema="cameo_full",
                n_chemicals=int(n_chem),
                n_nfpa=int(n_nfpa),
            )
        elif "cameo_nfpa" in tables:
            n_nfpa = con.execute("SELECT COUNT(*) FROM cameo_nfpa").fetchone()[0]
            ver = None
            if "cameo_meta" in tables:
                row = con.execute("SELECT value FROM cameo_meta WHERE key='version'").fetchone()
                ver = row[0] if row else None
            out.update(
                available=True,
                version=ver or "bundled",
                schema="cameo_nfpa",
                n_chemicals=int(n_nfpa),
                n_nfpa=int(n_nfpa),
            )
        con.close()
    except sqlite3.Error:
        out["available"] = False
    return out


def _is_mixture_name(name: str) -> bool:
    return bool(_MIXTURE_RE.search(name or ""))


def _rank(row: sqlite3.Row) -> tuple:
    name = row["name"] or ""
    has_nfpa = row["nfpa_health"] is not None or row["nfpa_flam"] is not None
    sort = row["sort"] if row["sort"] is not None else 99
    return (
        1 if _is_mixture_name(name) else 0,
        0 if has_nfpa else 1,
        int(sort),
        len(name),
        int(row["id"]),
    )


def _pack(
    *,
    name: str | None,
    chem_id: int | None,
    health: Any,
    flame: Any,
    react: Any,
    special: Any,
    source: Any,
    cas_id: str | None,
    mixture: bool,
) -> dict[str, Any]:
    cid = int(chem_id) if chem_id is not None else None
    return {
        "name": name,
        "chem_id": cid,
        "nfpa_health": int(health) if health is not None else None,
        "nfpa_flame": int(flame) if flame is not None else None,
        "nfpa_instability": int(react) if react is not None else None,
        "nfpa_special": (special or None),
        "nfpa_source": (source or None),
        "datasheet_url": DATASHEET_URL.format(id=cid) if cid is not None else None,
        "mixture": mixture,
        "cas_id": cas_id,
    }


def lookup_cameo(cas: str, *, sqlite_path: Path | None = None) -> Optional[dict[str, Any]]:
    """Return CAMEO chemical record for CAS, or None.

    NFPA ints are 0-4 or None (CAMEO has no diamond for that substance).
    """
    path = sqlite_path or resolve_cameo_sqlite()
    if not path:
        return None
    cas_disp = format_cas_display(cas)
    digits = normalize_cas(cas)
    if not cas_disp and not digits:
        return None
    try:
        con = _connect(path)
        tables = _table_names(con)
        if _is_full_schema(tables):
            rows = con.execute(
                """
                SELECT c.id, c.name, c.nfpa_health, c.nfpa_flam, c.nfpa_react,
                       c.nfpa_special, c.nfpa_source, cc.sort, cc.cas_id
                FROM chemical_cas cc
                JOIN chemicals c ON c.id = cc.chem_id
                WHERE cc.cas_id = ? OR cc.cas_nodash = ?
                """,
                (cas_disp, digits),
            ).fetchall()
            con.close()
            if not rows:
                return None
            best = sorted(rows, key=_rank)[0]
            packed = _pack(
                name=best["name"],
                chem_id=best["id"],
                health=best["nfpa_health"],
                flame=best["nfpa_flam"],
                react=best["nfpa_react"],
                special=best["nfpa_special"],
                source=best["nfpa_source"],
                cas_id=best["cas_id"],
                mixture=_is_mixture_name(best["name"] or ""),
            )
            if packed.get("mixture"):
                return None
            return packed
        if "cameo_nfpa" in tables:
            row = con.execute(
                """
                SELECT cas, name, chem_id, nfpa_health, nfpa_flam, nfpa_react,
                       nfpa_special, nfpa_source
                FROM cameo_nfpa
                WHERE cas = ? OR cas_nodash = ?
                """,
                (cas_disp, digits),
            ).fetchone()
            con.close()
            if not row:
                return None
            packed = _pack(
                name=row["name"],
                chem_id=row["chem_id"],
                health=row["nfpa_health"],
                flame=row["nfpa_flam"],
                react=row["nfpa_react"],
                special=row["nfpa_special"],
                source=row["nfpa_source"],
                cas_id=row["cas"],
                mixture=_is_mixture_name(row["name"] or ""),
            )
            if packed.get("mixture"):
                return None
            return packed
        con.close()
    except sqlite3.Error:
        return None
    return None


def cameo_hazard_metrics(cas: str) -> list[str]:
    """P2OASys scorer strings: ``'{n} - Health (CAMEO Chemicals)'``."""
    hit = lookup_cameo(cas)
    if not hit:
        return []
    out: list[str] = []
    if hit.get("nfpa_health") is not None:
        out.append(f"{hit['nfpa_health']} - Health (CAMEO Chemicals)")
    if hit.get("nfpa_flame") is not None:
        out.append(f"{hit['nfpa_flame']} - Fire (CAMEO Chemicals)")
    if hit.get("nfpa_instability") is not None:
        out.append(f"{hit['nfpa_instability']} - Instability (CAMEO Chemicals)")
    return out


def cameo_extra_sources(cas: str) -> dict[str, Any]:
    nfpa = cameo_hazard_metrics(cas)
    if not nfpa:
        return {}
    return {"hazard_metrics": {"nfpa": nfpa}}


def _pubchem_nfpa_texts(pubchem_nfpa: Any) -> list[str]:
    if isinstance(pubchem_nfpa, list):
        return [str(x).strip() for x in pubchem_nfpa if str(x).strip()]
    if pubchem_nfpa:
        return [s.strip() for s in str(pubchem_nfpa).split(";") if s.strip()]
    return []


def nfpa_property_rows(cas: str, pubchem_nfpa: Any = None) -> list[dict[str, str]]:
    """Key Properties rows: CAMEO 0–4 diamonds, plus PubChem NFPA text if present."""
    rows: list[dict[str, str]] = []
    hit = lookup_cameo(cas)
    if hit:
        src = str(hit.get("nfpa_source") or "CAMEO Chemicals")
        def _cell(val: Any) -> str:
            return "—" if val is None else str(val)
        rows.append(
            {"Property": "NFPA Health", "Value": _cell(hit.get("nfpa_health")), "Unit": "0–4", "Observations": src}
        )
        rows.append(
            {"Property": "NFPA Fire", "Value": _cell(hit.get("nfpa_flame")), "Unit": "0–4", "Observations": src}
        )
        if hit.get("nfpa_instability") is not None:
            rows.append(
                {
                    "Property": "NFPA Instability",
                    "Value": str(hit["nfpa_instability"]),
                    "Unit": "0–4",
                    "Observations": src,
                }
            )
        if hit.get("nfpa_special"):
            rows.append(
                {
                    "Property": "NFPA Special",
                    "Value": str(hit["nfpa_special"]),
                    "Unit": "—",
                    "Observations": src,
                }
            )
    texts = _pubchem_nfpa_texts(pubchem_nfpa)
    if texts:
        rows.append(
            {
                "Property": "NFPA (PubChem)",
                "Value": " | ".join(texts),
                "Unit": "—",
                "Observations": "PubChem PUG View",
            }
        )
    return rows


def main() -> None:
    st = cameo_status()
    print(f"available={st['available']} schema={st['schema']} version={st['version']}")
    print(f"path={st['path']}")
    print(f"chemicals={st['n_chemicals']} with_nfpa={st['n_nfpa']}")
    for cas in ("67-64-1", "67-66-3", "75-09-2", "124-38-9", "64-17-5", "7732-18-5"):
        print(cas, lookup_cameo(cas))


if __name__ == "__main__":
    main()
