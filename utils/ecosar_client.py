"""
EPI Suite / ECOSAR client for aquatic endpoints (P2OASys Ecological gap-fill).

Uses the EPI Suite CLI HTTP API (ECOSAR v2.20 organics), not the local Win32
Ecowinnt.exe GUI (ECOSAR v1.11) and not OPERA.

    GET  {base}/submit?cas=000071-43-2&modules=ecosar.organics
    POST {base}/submit/batch   JSON array of {cas|smiles, modules:[...]}

Environment:
    HAZQUERY_EPISUITE_API_BASE — default https://episuite.dev/api
    HAZQUERY_EPISUITE_API_KEY  — optional Bearer token
    HAZQUERY_SKIP_ECOSAR       — 1/true to disable network calls
    ECOSAR_CACHE_DB_PATH       — optional SQLite cache path

See docs/ECOSAR_EPI_SUITE_FEASIBILITY.md.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Optional

import requests

_LOG = logging.getLogger(__name__)

ECOSAR_CLIENT_VERSION = "ecosar_client_v1"

DEFAULT_API_BASE = "https://episuite.dev/api"
ORGANICS_MODULE = "ecosar.organics"

# P2OASys-relevant acute/chronic aquatic taxa (exclude earthworm/mysid by default)
_DEFAULT_ORGANISMS = frozenset({"Fish", "Daphnid", "Green Algae"})
_ACUTE_ENDPOINTS = frozenset({"LC50", "EC50"})
_CHRONIC_ENDPOINTS = frozenset({"ChV", "NOEC"})

_CAS_RE = re.compile(r"^(\d{2,7})-(\d{2})-(\d)$")
_FALSEY = frozenset({"0", "false", "no", "off", ""})


def pad_cas(cas: str) -> str:
    """Normalize CAS to zero-padded left segment (EPI Suite style)."""
    cas = (cas or "").strip()
    m = _CAS_RE.match(cas)
    if not m:
        return cas
    return f"{int(m.group(1)):06d}-{m.group(2)}-{m.group(3)}"


def api_base_url() -> str:
    raw = (os.environ.get("HAZQUERY_EPISUITE_API_BASE") or "").strip()
    return (raw or DEFAULT_API_BASE).rstrip("/")


def is_ecosar_enabled() -> bool:
    """False when HAZQUERY_SKIP_ECOSAR / HAZQUERY_ECOSAR disables the client."""
    skip = (os.environ.get("HAZQUERY_SKIP_ECOSAR") or "").strip().lower()
    if skip in ("1", "true", "yes", "on"):
        return False
    flag = (os.environ.get("HAZQUERY_ECOSAR") or "1").strip().lower()
    return flag not in _FALSEY


def is_ecosar_api_available(*, timeout: float = 8.0) -> bool:
    """True if OpenAPI root responds (network / local JAR reverse-proxy)."""
    if not is_ecosar_enabled():
        return False
    try:
        r = requests.get(f"{api_base_url()}/", timeout=timeout)
        if r.status_code != 200:
            return False
        data = r.json()
        return isinstance(data, dict) and "openapi" in data
    except Exception as exc:
        _LOG.debug("ECOSAR API unavailable: %s", exc)
        return False


def _headers() -> dict[str, str]:
    key = (os.environ.get("HAZQUERY_EPISUITE_API_KEY") or "").strip()
    return {"Authorization": f"Bearer {key}"} if key else {}


def default_cache_db_path() -> Path:
    env = (os.environ.get("ECOSAR_CACHE_DB_PATH") or "").strip()
    if env:
        return Path(env)
    try:
        import config as _cfg

        cfg_path = (getattr(_cfg, "ECOSAR_CACHE_DB_PATH", None) or "").strip()
        if cfg_path:
            return Path(cfg_path)
        data_dir = Path(getattr(_cfg, "DATA_DIR", "") or "")
        if data_dir and str(data_dir) != ".":
            return data_dir / "ecosar_cache.sqlite"
    except Exception:
        pass
    here = Path(__file__).resolve()
    # utils/ecosar_client.py → app root; hazquery/ecosar_client.py → hazquery root
    app_root = here.parents[1] if here.parent.name == "utils" else here.parent
    return app_root / "data" / "ecosar_cache.sqlite"


def _cache_connect(db: Path | None = None) -> sqlite3.Connection:
    path = Path(db) if db else default_cache_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ecosar_organics (
            cache_key TEXT PRIMARY KEY,
            cas TEXT,
            smiles TEXT,
            payload_json TEXT NOT NULL,
            fetched_at REAL NOT NULL,
            client TEXT
        )
        """
    )
    return con


def _cache_key(*, cas: str | None, smiles: str | None) -> str:
    parts = []
    if cas:
        parts.append(f"cas={pad_cas(cas)}")
    if smiles:
        parts.append(f"smiles={smiles.strip()}")
    parts.append(f"mod={ORGANICS_MODULE}")
    return "|".join(parts) or "empty"


def cache_get(
    *,
    cas: str | None = None,
    smiles: str | None = None,
    db: Path | None = None,
) -> dict[str, Any] | None:
    key = _cache_key(cas=cas, smiles=smiles)
    try:
        with _cache_connect(db) as con:
            row = con.execute(
                "SELECT payload_json FROM ecosar_organics WHERE cache_key = ?",
                (key,),
            ).fetchone()
        if not row:
            return None
        data = json.loads(row[0])
        if isinstance(data, dict):
            data["from_cache"] = True
            return data
    except Exception as exc:
        _LOG.debug("ECOSAR cache read failed: %s", exc)
    return None


def cache_put(
    result: dict[str, Any],
    *,
    cas: str | None = None,
    smiles: str | None = None,
    db: Path | None = None,
) -> None:
    if not result or not result.get("ok"):
        return
    # Key must match the request identity used by cache_get (do not mix in
    # response SMILES when the caller only asked by CAS — that caused misses).
    key_cas = cas or result.get("cas")
    key_smiles = smiles
    if not key_cas and not key_smiles:
        key_smiles = result.get("smiles")
    key = _cache_key(cas=key_cas, smiles=key_smiles)
    try:
        slim = {k: v for k, v in result.items() if k != "raw"}
        # Keep a compact raw ecosar block for audit, drop full tree if huge.
        raw = result.get("raw")
        if isinstance(raw, dict) and isinstance(raw.get("ecosar"), dict):
            slim["raw_ecosar"] = {
                "modelResults": (raw["ecosar"].get("modelResults") or [])[:80],
                "alerts": raw["ecosar"].get("alerts"),
            }
        with _cache_connect(db) as con:
            con.execute(
                """
                INSERT OR REPLACE INTO ecosar_organics
                (cache_key, cas, smiles, payload_json, fetched_at, client)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    pad_cas(key_cas) if key_cas else "",
                    (key_smiles or ""),
                    json.dumps(slim, default=str),
                    time.time(),
                    ECOSAR_CLIENT_VERSION,
                ),
            )
            con.commit()
    except Exception as exc:
        _LOG.debug("ECOSAR cache write failed: %s", exc)


def submit_organics(
    *,
    cas: str | None = None,
    smiles: str | None = None,
    timeout: float = 60.0,
    use_cache: bool = True,
) -> dict[str, Any]:
    """
    Run ECOSAR organics for one chemical.

    Returns ``{"ok", "error", "raw", "rows", "acute_mg_l", "chronic_chv_mg_l", ...}``.
    Does not invent P2OASys scores.
    """
    if not is_ecosar_enabled():
        return {"ok": False, "error": "ecosar_disabled", "rows": []}
    if not cas and not smiles:
        return {"ok": False, "error": "cas_or_smiles_required", "rows": []}
    if use_cache:
        hit = cache_get(cas=cas, smiles=smiles)
        if hit and hit.get("ok"):
            hit = dict(hit)
            hit["from_cache"] = True
            return hit
    params: dict[str, str] = {"modules": ORGANICS_MODULE}
    if cas:
        params["cas"] = pad_cas(cas)
    if smiles:
        params["smiles"] = smiles.strip()
    try:
        r = requests.get(
            f"{api_base_url()}/submit",
            params=params,
            headers=_headers(),
            timeout=timeout,
        )
        r.raise_for_status()
        raw = r.json()
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "rows": []}
    packaged = _package_result(raw, cas=cas, smiles=smiles)
    if use_cache and packaged.get("ok"):
        cache_put(packaged, cas=cas, smiles=smiles)
    return packaged


def submit_organics_batch(
    items: Iterable[dict[str, str]],
    *,
    timeout: float = 120.0,
) -> dict[str, Any]:
    """
    Batch organics estimates (max 100 per API).

    Each item: ``{"cas": "..."}`` and/or ``{"smiles": "..."}``.
    """
    if not is_ecosar_enabled():
        return {"ok": False, "error": "ecosar_disabled", "results": []}
    body = []
    for it in items:
        entry: dict[str, Any] = {"modules": [ORGANICS_MODULE]}
        if it.get("cas"):
            entry["cas"] = pad_cas(it["cas"])
        if it.get("smiles"):
            entry["smiles"] = it["smiles"].strip()
        body.append(entry)
    if not body:
        return {"ok": False, "error": "empty_batch", "results": []}
    try:
        r = requests.post(
            f"{api_base_url()}/submit/batch",
            json=body,
            headers=_headers(),
            timeout=timeout,
        )
        r.raise_for_status()
        payload = r.json()
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "results": []}

    packaged = []
    for idx, raw in enumerate(payload.get("results") or []):
        if raw is None:
            packaged.append({"ok": False, "error": "null_result", "index": idx, "rows": []})
            continue
        item = _package_result(raw, index=idx)
        if item.get("ok"):
            cache_put(item, cas=item.get("cas"), smiles=item.get("smiles"))
        packaged.append(item)
    return {
        "ok": bool(payload.get("success")),
        "count": payload.get("count"),
        "errors": payload.get("errors") or [],
        "results": packaged,
        "client": ECOSAR_CLIENT_VERSION,
    }


def extract_model_rows(
    ecosar_block: dict[str, Any] | None,
    *,
    organisms: frozenset[str] = _DEFAULT_ORGANISMS,
) -> list[dict[str, Any]]:
    """Flatten ``ecosar.modelResults`` to P2OASys-relevant aquatic rows."""
    if not isinstance(ecosar_block, dict):
        return []
    out: list[dict[str, Any]] = []
    for row in ecosar_block.get("modelResults") or []:
        if not isinstance(row, dict):
            continue
        org = str(row.get("organism") or "")
        if organisms and org not in organisms:
            continue
        end = str(row.get("endpoint") or "")
        conc = row.get("concentration")
        try:
            conc_f = float(conc) if conc is not None else None
        except (TypeError, ValueError):
            conc_f = None
        out.append(
            {
                "qsar_class": row.get("qsarClass"),
                "organism": org,
                "duration": row.get("duration") or "",
                "endpoint": end,
                "concentration_mg_l": conc_f,
                "max_log_kow": row.get("maxLogKow"),
                "flags": row.get("flags") or [],
            }
        )
    return out


def conservative_endpoint_min(
    rows: list[dict[str, Any]],
    endpoints: frozenset[str],
) -> Optional[float]:
    vals: list[float] = []
    for r in rows:
        if str(r.get("endpoint") or "") not in endpoints:
            continue
        v = r.get("concentration_mg_l")
        if isinstance(v, (int, float)) and v > 0:
            vals.append(float(v))
    return min(vals) if vals else None


def _package_result(
    raw: dict[str, Any],
    *,
    cas: str | None = None,
    smiles: str | None = None,
    index: int | None = None,
) -> dict[str, Any]:
    ecosar = raw.get("ecosar") if isinstance(raw, dict) else None
    rows = extract_model_rows(ecosar if isinstance(ecosar, dict) else None)
    acute = conservative_endpoint_min(rows, _ACUTE_ENDPOINTS)
    chronic = conservative_endpoint_min(rows, _CHRONIC_ENDPOINTS)
    params = (raw.get("parameters") or {}) if isinstance(raw, dict) else {}
    return {
        "ok": True,
        "error": None,
        "index": index,
        "cas": params.get("cas") or (pad_cas(cas) if cas else None),
        "smiles": params.get("smiles") or smiles,
        "rows": rows,
        "acute_mg_l": acute,
        "chronic_chv_mg_l": chronic,
        "alerts": (ecosar or {}).get("alerts") if isinstance(ecosar, dict) else None,
        "raw": raw,
        "client": ECOSAR_CLIENT_VERSION,
        "source": "ECOSAR_v2.20_episuite_api",
        "predicted": True,
        "from_cache": False,
    }


def has_measured_aquatic_lc50(existing_hazard: dict[str, Any] | None) -> bool:
    """True if hazard/extra already carries acute aquatic LC50/EC50 (mg/L)."""
    if not existing_hazard:
        return False
    for key in ("lc50_aquatic_mg_l", "aquatic_toxicity"):
        v = existing_hazard.get(key)
        if isinstance(v, (int, float)) and v > 0:
            return True
        if isinstance(v, dict):
            inner = v.get("value")
            if isinstance(inner, (int, float)) and inner > 0:
                return True
    blob = " ".join(str(t.get("value") or "") for t in (existing_hazard.get("toxicities") or [])).lower()
    if ("lc50" in blob or "ec50" in blob) and "mg/l" in blob:
        # Prefer treating existing aquatic evidence as measured/stronger than ECOSAR.
        if any(
            k in blob
            for k in (
                "fish",
                "daphn",
                "algae",
                "aquatic",
                "trout",
                "fathead",
                "invertebrate",
            )
        ):
            return True
        # Plain "LC50 X mg/L" from SDS aquatic bridge also counts.
        if re.search(r"(?:lc50|ec50)\s*[\d.]+\s*mg/l", blob):
            return True
    return False


def ecosar_to_extra_sources(
    result: dict[str, Any] | None,
    *,
    existing_hazard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Bridge ECOSAR package into P2OASys ``extra_sources``.

    Emits toxicity strings consumable by ``_extract_lc50_aquatic`` plus structured
    acute/chronic fields. Gap-fills only when aquatic LC50/EC50 is absent.
    Does **not** invent integer P2OASys scores.
    """
    if not result or not result.get("ok"):
        return {}
    if has_measured_aquatic_lc50(existing_hazard):
        return {}

    toxicities: list[dict[str, Any]] = []
    notes: list[str] = []
    acute = result.get("acute_mg_l")
    chronic = result.get("chronic_chv_mg_l")

    if acute is not None:
        toxicities.append(
            {
                "value": f"LC50 {acute} mg/L (aquatic; ECOSAR organics min Fish/Daphnid/Algae)",
                "unit": "mg/L",
                "species_route": ["aquatic", "predicted"],
                "source": "ECOSAR_v2.20",
                "predicted": True,
            }
        )
        notes.append(f"ECOSAR organics acute min LC50/EC50={acute} mg/L (predicted)")

    if chronic is not None:
        toxicities.append(
            {
                "value": f"ChV {chronic} mg/L (aquatic; ECOSAR organics min Fish/Daphnid/Algae)",
                "unit": "mg/L",
                "species_route": ["aquatic", "predicted", "chronic"],
                "source": "ECOSAR_v2.20",
                "predicted": True,
            }
        )
        notes.append(f"ECOSAR organics chronic min ChV={chronic} mg/L (predicted)")

    if not toxicities:
        return {}

    out: dict[str, Any] = {
        "toxicities": toxicities,
        "_pipeline_notes": notes,
        "ecosar_meta": {
            "source": result.get("source"),
            "client": result.get("client"),
            "predicted": True,
            "from_cache": bool(result.get("from_cache")),
            "acute_mg_l": acute,
            "chronic_chv_mg_l": chronic,
            "alerts": result.get("alerts"),
            "n_rows": len(result.get("rows") or []),
        },
    }
    if acute is not None:
        out["lc50_aquatic_mg_l"] = acute
        out["aquatic_toxicity"] = acute
    if chronic is not None:
        out["aquatic_chv_mg_l"] = chronic
    return out


def fetch_ecosar_extra_sources(
    cas: str | None = None,
    *,
    smiles: str | None = None,
    existing_hazard: dict[str, Any] | None = None,
    timeout: float = 60.0,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Submit (or cache-hit) then bridge to ``extra_sources`` with gap-fill."""
    if not is_ecosar_enabled():
        return {}
    if has_measured_aquatic_lc50(existing_hazard):
        return {}
    if not cas and not smiles:
        return {}
    result = submit_organics(cas=cas, smiles=smiles, timeout=timeout, use_cache=use_cache)
    return ecosar_to_extra_sources(result, existing_hazard=existing_hazard)


# Back-compat alias from POC
ecosar_to_extra_sources_sketch = ecosar_to_extra_sources
