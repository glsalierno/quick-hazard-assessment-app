"""
Batch OPERA 2.9 runs, PubChem CAS→SMILES helpers, endpoint metadata, and optional SQLite cache.

Designed for automation (e.g. fastp2oasys): one MATLAB/OPERA process per ``.smi`` file, not per compound.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from urllib.parse import quote

import pandas as pd
import requests

from utils import opera_client

_LOG = logging.getLogger(__name__)

PUBCHEM_HOST = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"


# Human-readable hints for common OPERA 2.9 CSV columns (extend as needed).
COLUMN_DESCRIPTIONS: dict[str, str] = {
    "MoleculeID": "User / file identifier for each row (from .smi second column)",
    "MolWeight": "Molecular weight (OPERA structural property)",
    "LogP_pred": "Octanol–water partition coefficient (log Kow)",
    "MP_pred": "Melting point (°C)",
    "BP_pred": "Boiling point (°C)",
    "LogWS_pred": "Aqueous solubility (log mol/L at 25 °C)",
    "LogBCF_pred": "Fish bioconcentration factor (log)",
    "LogVP_pred": "Vapor pressure (log mmHg)",
    "LogHL_pred": "Henry’s law constant (log atm·m³/mol)",
    "LogKOA_pred": "Octanol–air partition coefficient (log)",
    "LogOH_pred": "OH radical reaction rate (log cm³/molecule·s)",
    "CATMoS_LD50_pred": "CATMoS acute oral LD50 (mg/kg)",
    "CATMoS_GHS_pred": "CATMoS GHS acute toxicity category",
    "CATMoS_EPA_pred": "CATMoS EPA acute toxicity category",
    "CERAPP_Bind_pred": "CERAPP estrogen receptor binding",
    "CoMPARA_Bind_pred": "CoMPARA androgen receptor binding",
}

# Extra glosses for structural / count columns (OPERA PaDEL- / CDK-style names).
_STRUCTURAL_COLUMNS: dict[str, str] = {
    "nbAtoms": "Total atom count (structure descriptor)",
    "nbHeavyAtoms": "Non-hydrogen atom count (structure descriptor)",
    "nbC": "Carbon atom count",
    "nbO": "Oxygen atom count",
    "nbN": "Nitrogen atom count",
    "nbAromAtom": "Aromatic atom count",
    "nbRing": "Ring count",
    "nbHeteroRing": "Heterocyclic ring count",
    "Sp3Sp2HybRatio": "sp³ / sp² carbon ratio (shape descriptor)",
    "nbRotBd": "Rotatable bond count (flexibility)",
    "nbHBdAcc": "H-bond acceptor count (Lipinski-related)",
    "ndHBdDon": "H-bond donor count (Lipinski-related)",
    "nbLipinskiFailures": "Lipinski rule-of-five failure count",
    "TopoPolSurfAir": "Topological polar surface area (Å²)",
    "MolarRefract": "Molar refractivity (cm³/mol)",
    "CombDipolPolariz": "Combined dipole moment × polarizability descriptor",
}


def clarify_opera_endpoint(name: str) -> str:
    """
    One-line clarification for an OPERA 2.9 CSV column (prediction, AD, confidence, or descriptor).
    """
    n = (name or "").strip()
    if not n:
        return ""
    if n in COLUMN_DESCRIPTIONS:
        return COLUMN_DESCRIPTIONS[n]
    if n in _STRUCTURAL_COLUMNS:
        return _STRUCTURAL_COLUMNS[n]
    if n.endswith("_predRange"):
        pred = n[: -len("_predRange")] + "_pred"
        return f"Numeric interval or uncertainty range paired with {pred} (OPERA)"
    if n.startswith("AD_index_"):
        prop = n[len("AD_index_") :]
        return f"Applicability-domain distance index for {prop} (lower = closer to training domain)"
    if n.startswith("Conf_index_"):
        prop = n[len("Conf_index_") :]
        return f"Model confidence index for {prop} predictions (OPERA)"
    if n.startswith("AD_"):
        prop = n[3:]
        return f"Applicability-domain outcome for {prop} models (inside/outside domain, OPERA)"
    if n == "ionization":
        return "Ionization / speciation hint used with pKa and LogD models (OPERA)"
    if "_pred" in n and n.endswith("_pred"):
        stem = n[: -len("_pred")]
        return f"QSAR prediction for {stem} (OPERA model output)"
    return "OPERA 2.9 batch CSV column (see NIEHS OPERA documentation for detail)"


def _normalize_cas(cas: str) -> str:
    return (cas or "").strip()


def _pubchem_get(url: str, *, timeout: float = 45.0, max_retries: int = 4) -> requests.Response:
    delay = 0.5
    last: requests.Response | None = None
    for attempt in range(max_retries):
        r = requests.get(url, timeout=timeout)
        last = r
        if r.status_code == 429:
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
            continue
        return r
    assert last is not None
    return last


def cas_to_smiles(cas_list: list[str]) -> dict[str, Optional[str]]:
    """
    Resolve each CAS to Canonical SMILES via PubChem ``compound/name/.../property/CanonicalSMILES/TXT``.

    Uses ``time.sleep(0.5)`` between calls to reduce rate-limit risk. ``404`` → ``None``.
    Keys in the returned dict are **normalized** CAS strings (stripped input).
    """
    out: dict[str, Optional[str]] = {}
    for raw in cas_list:
        cas = _normalize_cas(raw)
        if not cas:
            continue
        url = f"{PUBCHEM_HOST}/compound/name/{quote(cas, safe='')}/property/CanonicalSMILES/TXT"
        try:
            r = _pubchem_get(url)
        except requests.RequestException as e:
            _LOG.warning("PubChem request failed for CAS %s: %s", cas, e)
            out[cas] = None
            time.sleep(0.5)
            continue
        if r.status_code == 404:
            out[cas] = None
        elif r.ok:
            txt = (r.text or "").strip()
            out[cas] = txt if txt else None
        else:
            _LOG.debug("PubChem CAS %s → HTTP %s", cas, r.status_code)
            out[cas] = None
        time.sleep(0.5)
    return out


def _cas_to_primary_cid(cas: str) -> Optional[int]:
    url = f"{PUBCHEM_HOST}/compound/xref/RN/{quote(cas, safe='')}/cids/TXT"
    try:
        r = _pubchem_get(url)
    except requests.RequestException:
        return None
    if not r.ok:
        return None
    for line in (r.text or "").splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return None


def smiles_from_cas_batch(cas_list: list[str]) -> dict[str, Optional[str]]:
    """
    Map CAS → Canonical SMILES using **CID xref** then **batched** ``cid/property/CanonicalSMILES/CSV``.

    Fewer HTTP round-trips than :func:`cas_to_smiles` for long lists. Unknown CAS → ``None``.
    Keys are normalized CAS strings.
    """
    uniq: list[str] = []
    seen: set[str] = set()
    for raw in cas_list:
        c = _normalize_cas(raw)
        if not c or c in seen:
            continue
        seen.add(c)
        uniq.append(c)

    cas_cid: dict[str, Optional[int]] = {}
    for cas in uniq:
        cas_cid[cas] = _cas_to_primary_cid(cas)
        time.sleep(0.25)

    cid_to_cas: dict[int, str] = {}
    ordered_cids: list[int] = []
    for cas, cid in cas_cid.items():
        if cid is None:
            continue
        if cid not in cid_to_cas:
            cid_to_cas[cid] = cas
            ordered_cids.append(cid)
        else:
            # duplicate CID for two CAS — rare; keep first CAS for batch property row
            _LOG.debug("Duplicate PubChem CID %s for CAS %s (already %s)", cid, cas, cid_to_cas[cid])

    out: dict[str, Optional[str]] = {cas: None for cas in uniq}
    chunk_size = 200
    for i in range(0, len(ordered_cids), chunk_size):
        chunk = ordered_cids[i : i + chunk_size]
        url = f"{PUBCHEM_HOST}/compound/cid/property/CanonicalSMILES/CSV"
        try:
            r = requests.post(
                url,
                data={"cid": ",".join(str(c) for c in chunk)},
                timeout=120,
                headers={"User-Agent": "hazquery-opera-batch/1.0"},
            )
        except requests.RequestException as e:
            _LOG.warning("PubChem batch property request failed: %s", e)
            time.sleep(0.5)
            continue
        if not r.ok:
            _LOG.warning("PubChem batch SMILES HTTP %s", r.status_code)
            time.sleep(0.5)
            continue
        try:
            df = pd.read_csv(pd.io.common.StringIO(r.text))
        except Exception as e:
            _LOG.warning("Failed to parse PubChem CSV: %s", e)
            continue
        cid_col = "CID" if "CID" in df.columns else df.columns[0]
        smi_col = "CanonicalSMILES" if "CanonicalSMILES" in df.columns else df.columns[-1]
        for _, row in df.iterrows():
            try:
                cid = int(row[cid_col])
            except (TypeError, ValueError):
                continue
            cas0 = cid_to_cas.get(cid)
            if not cas0:
                continue
            smi = row.get(smi_col)
            out[cas0] = (str(smi).strip() if smi is not None and str(smi) != "nan" else None)
        time.sleep(0.5)

    for cas in uniq:
        if cas not in out:
            out[cas] = None
    return out


def _smiles_file_hash(lines: list[tuple[str, str]]) -> str:
    h = hashlib.sha256()
    for smi, mid in lines:
        h.update((smi + "\t" + mid + "\n").encode("utf-8"))
    return h.hexdigest()


def _default_data_dir() -> Path:
    try:
        import config as _cfg

        return Path(getattr(_cfg, "DATA_DIR", Path(__file__).resolve().parents[1] / "data"))
    except Exception:
        return Path(__file__).resolve().parents[1] / "data"


def default_endpoints_json_path() -> Path:
    return _default_data_dir() / "opera_endpoints.json"


def default_opera_cache_db_path() -> Path:
    return _default_data_dir() / "opera_batch_cache.sqlite"


def load_endpoint_metadata(path: Path | None = None) -> list[dict[str, str]]:
    """Load ``opera_endpoints.json`` (list of {name, description}) if present."""
    p = path or default_endpoints_json_path()
    if not p.is_file():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict) and "name" in x]
        if isinstance(data, dict):
            cols = data.get("columns")
            if isinstance(cols, list) and cols:
                return [x for x in cols if isinstance(x, dict) and "name" in x]
            names = data.get("column_names_only")
            if isinstance(names, list) and names:
                return build_endpoints_json_from_columns(names)
    except Exception as e:
        _LOG.warning("Could not read %s: %s", p, e)
    return []


def get_all_opera_endpoints(json_path: Path | None = None) -> list[dict[str, str]]:
    """
    Load ``data/opera_endpoints.json`` and return every column with a display label.

    Each item is ``{"name": str, "label": str, "description": str}``. The **label** is the
    human-readable description when present and non-generic; otherwise the **name** is used.
    """
    meta = load_endpoint_metadata(json_path or default_endpoints_json_path())
    out: list[dict[str, str]] = []
    for m in meta:
        name = (m.get("name") or "").strip()
        if not name:
            continue
        desc = (m.get("description") or "").strip()
        generic = desc.lower() in ("", "opera output column")
        label = clarify_opera_endpoint(name) if generic or not desc else desc
        out.append({"name": name, "label": label, "description": desc if desc else clarify_opera_endpoint(name)})
    return out


def get_endpoint_list(
    *,
    opera_exe: Path | str | None = None,
    cache_json: Path | None = None,
    refresh: bool = False,
) -> list[str]:
    """
    Return OPERA CSV column names (prediction / descriptor columns).

    If ``cache_json`` exists and ``refresh`` is False, reads headers from that JSON.
    Otherwise runs a minimal OPERA job (aspirin) and extracts the CSV header.
    """
    p = cache_json or default_endpoints_json_path()
    if p.is_file() and not refresh:
        meta = load_endpoint_metadata(p)
        names = [m["name"] for m in meta if m.get("name")]
        if names:
            return names

    aspirin = "CC(=O)OC1=CC=CC=C1C(=O)O"
    br = batch_predict(
        [aspirin],
        molecule_ids=["discovery"],
        opera_exe=opera_exe,
        disk_cache=False,
        sqlite_cache_path=None,
        fallback_single_on_error=False,
    )
    return list(br.df.columns)


@dataclass
class OperaBatchResult:
    """Outcome of :func:`batch_predict`."""

    df: pd.DataFrame
    by_smiles: dict[str, dict[str, str]]
    errors: list[str] = field(default_factory=list)
    cache_hit: bool = False


class OperaCasCache:
    """SQLite store: CAS + SMILES → skip re-running OPERA when inputs match."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS opera_cas_row (
                    cas TEXT PRIMARY KEY,
                    smiles TEXT NOT NULL,
                    smiles_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated REAL NOT NULL
                )
                """
            )
            con.commit()

    def get_row(self, cas: str, smiles: str) -> Optional[dict[str, Any]]:
        cas_n = _normalize_cas(cas)
        sh = hashlib.sha256(smiles.encode("utf-8")).hexdigest()
        with sqlite3.connect(self.path) as con:
            cur = con.execute(
                "SELECT payload_json FROM opera_cas_row WHERE cas = ? AND smiles_hash = ?",
                (cas_n, sh),
            )
            row = cur.fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def put_row(self, cas: str, smiles: str, payload: dict[str, Any]) -> None:
        cas_n = _normalize_cas(cas)
        sh = hashlib.sha256(smiles.encode("utf-8")).hexdigest()
        blob = json.dumps(payload, ensure_ascii=False)
        now = time.time()
        with sqlite3.connect(self.path) as con:
            con.execute(
                """
                INSERT INTO opera_cas_row (cas, smiles, smiles_hash, payload_json, updated)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(cas) DO UPDATE SET
                    smiles=excluded.smiles,
                    smiles_hash=excluded.smiles_hash,
                    payload_json=excluded.payload_json,
                    updated=excluded.updated
                """,
                (cas_n, smiles, sh, blob, now),
            )
            con.commit()


def batch_predict(
    smiles_list: list[str],
    molecule_ids: list[str] | None = None,
    *,
    opera_exe: Path | str | None = None,
    java_home: str | None = None,
    timeout_seconds: int | None = None,
    disk_cache: bool = True,
    disk_cache_dir: Path | str | None = None,
    sqlite_cache_path: Path | str | None = None,
    cas_for_cache: list[str] | None = None,
    fallback_single_on_error: bool = True,
    progress_callback: Callable[[int, int], None] | None = None,
    use_tqdm: bool = False,
) -> OperaBatchResult:
    """
    Write all structures to one ``.smi`` file and run OPERA once.

    - **SQLite cache:** if ``sqlite_cache_path`` and ``cas_for_cache`` align with ``smiles_list``,
      cached rows are reused and only uncached structures are sent to OPERA.
    - **Disk cache:** optional reuse of full-run CSV keyed by SHA256 of ``.smi`` content.
    - **Resilience:** if the batch run fails and ``fallback_single_on_error`` is true, retries
      each SMILES individually and logs per-compound failures.

    Returns :class:`OperaBatchResult` with ``df`` (includes ``input_smiles`` and ``molecule_id``),
    ``by_smiles`` mapping input SMILES string → flat endpoint dict, and ``errors`` (human messages).
    """
    errors: list[str] = []
    clean_smiles = [(s or "").strip() for s in smiles_list]
    if not any(clean_smiles):
        return OperaBatchResult(df=pd.DataFrame(), by_smiles={}, errors=["empty_smiles_list"])

    n = len(clean_smiles)
    mids = molecule_ids if molecule_ids is not None else [f"row{i}" for i in range(n)]
    if len(mids) != n:
        errors.append("molecule_ids length mismatch; using row indices")
        mids = [f"row{i}" for i in range(n)]

    lines = [(s, mids[i]) for i, s in enumerate(clean_smiles) if s]
    if not lines:
        return OperaBatchResult(df=pd.DataFrame(), by_smiles={}, errors=["no_valid_smiles"])

    cache_hit = False
    disk_dir = Path(disk_cache_dir) if disk_cache_dir else (_default_data_dir() / "opera_run_cache")
    h = _smiles_file_hash(lines)
    cached_csv = disk_dir / f"{h}.csv"

    sql_cache: OperaCasCache | None = None
    if sqlite_cache_path and cas_for_cache and len(cas_for_cache) == n:
        sql_cache = OperaCasCache(sqlite_cache_path)

    rows_out: list[dict[str, Any]] = []
    pending_idx: list[int] = []
    for i, sm in enumerate(clean_smiles):
        if not sm:
            continue
        cas_i = _normalize_cas(cas_for_cache[i]) if cas_for_cache and i < len(cas_for_cache) else ""
        if sql_cache and cas_i:
            hit = sql_cache.get_row(cas_i, sm)
            if hit:
                hit = dict(hit)
                hit.pop("_opera_cache_cas", None)
                hit["input_smiles"] = sm
                hit["molecule_id"] = mids[i]
                hit["_opera_cache_cas"] = cas_i
                rows_out.append(hit)
                cache_hit = True
                continue
        pending_idx.append(i)

    all_smiles_indices = [i for i, s in enumerate(clean_smiles) if s]
    if (
        disk_cache
        and pending_idx
        and pending_idx == all_smiles_indices
        and cached_csv.is_file()
    ):
        try:
            df_hit = pd.read_csv(cached_csv)
            if len(df_hit) == len(pending_idx):
                cache_hit = True
                for j, ridx in enumerate(pending_idx):
                    row = df_hit.iloc[j].to_dict()
                    row["input_smiles"] = clean_smiles[ridx]
                    row["molecule_id"] = mids[ridx]
                    cas_j = (
                        _normalize_cas(cas_for_cache[ridx])
                        if cas_for_cache and ridx < len(cas_for_cache)
                        else ""
                    )
                    if cas_j:
                        row["_opera_cache_cas"] = cas_j
                    rows_out.append(row)
                pending_idx = []
        except Exception as e:
            _LOG.debug("Disk cache read failed: %s", e)

    def _notify(step: int, total: int) -> None:
        if progress_callback:
            progress_callback(step, total)

    def _run_indices(indices: list[int]) -> tuple[list[dict[str, Any]], list[str]]:
        if not indices:
            return [], []
        sub_lines = [(clean_smiles[i], mids[i]) for i in indices]
        tmp = tempfile.mkdtemp(prefix="hazquery_opera_batch_")
        err_local: list[str] = []
        try:
            smi_path = Path(tmp) / f"input_{int(time.time())}.smi"
            out_path = Path(tmp) / "opera_out.csv"
            smi_path.write_text(
                "\n".join(f"{smi}\t{mid}" for smi, mid in sub_lines) + "\n",
                encoding="utf-8",
            )
            batch = opera_client.run_opera_on_paths(
                smi_path,
                out_path,
                opera_exe=opera_exe,
                java_home=java_home,
                timeout_seconds=timeout_seconds,
                cwd=str(tmp),
            )
            if not batch["ok"]:
                err_local.append(batch.get("error") or "opera_failed")
                return [], err_local
            raw_rows = batch["rows"]
            if len(raw_rows) != len(indices):
                err_local.append(
                    f"row_count_mismatch: got {len(raw_rows)} OPERA rows for {len(indices)} inputs"
                )
            mapped: list[dict[str, Any]] = []
            for j, ridx in enumerate(indices):
                base = dict(raw_rows[j]) if j < len(raw_rows) else {}
                base["input_smiles"] = clean_smiles[ridx]
                base["molecule_id"] = mids[ridx]
                cas_j = (
                    _normalize_cas(cas_for_cache[ridx])
                    if cas_for_cache and ridx < len(cas_for_cache)
                    else ""
                )
                if cas_j:
                    base["_opera_cache_cas"] = cas_j
                mapped.append(base)
            if disk_cache and mapped and not cached_csv.exists() and len(indices) == len(lines):
                try:
                    disk_dir.mkdir(parents=True, exist_ok=True)
                    pd.DataFrame(raw_rows).to_csv(cached_csv, index=False)
                except Exception as e:
                    _LOG.debug("Disk cache write failed: %s", e)
            return mapped, err_local
        finally:
            import shutil

            shutil.rmtree(tmp, ignore_errors=True)

    new_rows: list[dict[str, Any]] = []
    batch_err: list[str] = []
    if pending_idx:
        total = len(pending_idx)
        _notify(0, total)
        if use_tqdm:
            try:
                from tqdm import tqdm  # type: ignore

                tqdm.write(f"Running OPERA batch ({total} structure(s))…")
            except Exception:
                pass
        new_rows, batch_err = _run_indices(pending_idx)
        _notify(total, total)
        if batch_err and not new_rows and fallback_single_on_error and len(pending_idx) > 1:
            _LOG.warning("OPERA batch failed; falling back to single-structure runs.")
            singles: list[dict[str, Any]] = []
            fb_iter = pending_idx
            if use_tqdm:
                try:
                    from tqdm import tqdm  # type: ignore

                    fb_iter = tqdm(pending_idx, desc="OPERA fallback")  # type: ignore[assignment]
                except Exception:
                    pass
            for step, ridx in enumerate(fb_iter, start=1):
                _notify(step, len(pending_idx))
                one_rows, one_err = _run_indices([ridx])
                if one_rows:
                    singles.extend(one_rows)
                else:
                    msg = f"SMILES index {ridx} failed: {one_err[0] if one_err else 'unknown'}"
                    errors.append(msg)
                    _LOG.warning(msg)
            new_rows = singles
        elif batch_err and not new_rows:
            errors.extend(batch_err)
        rows_out.extend(new_rows)

    if not rows_out:
        return OperaBatchResult(df=pd.DataFrame(), by_smiles={}, errors=errors or ["no_opera_rows"])

    if sql_cache and cas_for_cache and len(cas_for_cache) == n:
        for raw in rows_out:
            sm = str(raw.get("input_smiles", ""))
            cas_i = _normalize_cas(str(raw.get("_opera_cache_cas", "")))
            if cas_i and sm:
                try:
                    payload = {k: v for k, v in raw.items() if not str(k).startswith("_opera_")}
                    sql_cache.put_row(cas_i, sm, payload)
                except Exception as e:
                    _LOG.debug("SQLite cache put failed: %s", e)

    df = pd.DataFrame(rows_out)
    by_smiles: dict[str, dict[str, str]] = {}
    for _, row in df.iterrows():
        sm = str(row.get("input_smiles", ""))
        if sm:
            by_smiles[sm] = {
                k: str(v)
                for k, v in row.items()
                if pd.notna(v) and not str(k).startswith("_opera_")
            }

    return OperaBatchResult(df=df, by_smiles=by_smiles, errors=errors, cache_hit=cache_hit)


def predict_opera_batch(smiles_list: list[str], endpoint: str) -> dict[str, dict[str, Any]]:
    ep_raw = (endpoint or "").strip()
    col = opera_client._endpoint_to_column(ep_raw)
    if not col:
        return {"__error__": {"ok": False, "error": f"unknown_endpoint:{ep_raw}", "endpoint": ep_raw, "value": None}}

    br = batch_predict(smiles_list, fallback_single_on_error=True)
    if br.df.empty:
        err = br.errors[0] if br.errors else "empty_result"
        return {s: {"ok": False, "error": err, "endpoint": ep_raw, "column": col, "value": None} for s in smiles_list}

    out: dict[str, dict[str, Any]] = {}
    ad_key = opera_client._applicability_domain_key(col)
    for sm in smiles_list:
        sm = (sm or "").strip()
        if not sm:
            continue
        row = br.by_smiles.get(sm, {})
        if not row:
            out[sm] = {"ok": False, "error": "missing_row", "endpoint": ep_raw, "column": col, "value": None}
            continue
        val = row.get(col, "")
        ad = row.get(ad_key, "") if ad_key else None
        out[sm] = {
            "ok": True,
            "error": None,
            "endpoint": ep_raw,
            "column": col,
            "value": val,
            "applicability_domain": ad,
        }
    return out


def build_endpoints_json_from_columns(columns: Iterable[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for name in columns:
        if not name:
            continue
        desc = COLUMN_DESCRIPTIONS.get(name, "OPERA output column")
        out.append({"name": name, "description": desc})
    return out


# --- Example (command-line: python -m utils.opera_batch) ---
if __name__ == "__main__":
    demo = ["CC(=O)OC1=CC=CC=C1C(=O)O", "CN1C=NC2=C1C(=O)N(C(=O)N2C)C"]
    r = batch_predict(demo, molecule_ids=["aspirin", "caffeine"], disk_cache=False, sqlite_cache_path=None)
    print("errors:", r.errors)
    print(r.df[["input_smiles", "molecule_id", "LogP_pred"]].head() if "LogP_pred" in r.df.columns else r.df.head())
