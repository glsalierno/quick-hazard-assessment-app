"""
Build and write local QA validation snapshots for an assessed chemical (GHhaz6).

Writes under ``validation_snapshots/`` relative to the app root. Omits secrets,
absolute local paths, and oversized raw PubChem payloads.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from utils import data_formatter, ghs_formatter, qa_checks

_ABS_PATH_RE = re.compile(r"([A-Za-z]:[\\/][^\s\"']+|/(?:home|Users|tmp|var|opt)/[^\s\"']+)")
_SECRET_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "token",
        "secret",
        "password",
        "authorization",
        "openai_api_key",
        "hf_token",
    }
)


def default_snapshot_dir(app_root: Optional[Path] = None) -> Path:
    root = app_root or Path(__file__).resolve().parents[1]
    return root / "validation_snapshots"


def _scrub(obj: Any, *, depth: int = 0) -> Any:
    if depth > 12:
        return None
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            kl = str(k).lower()
            if kl in _SECRET_KEYS or any(s in kl for s in ("api_key", "password", "secret", "token")):
                continue
            if kl in ("raw_html", "raw_pug", "raw_response", "full_record", "binary"):
                continue
            out[str(k)] = _scrub(v, depth=depth + 1)
        return out
    if isinstance(obj, list):
        if len(obj) > 200:
            return [_scrub(x, depth=depth + 1) for x in obj[:200]] + [
                {"_truncated": f"{len(obj) - 200} more items omitted"}
            ]
        return [_scrub(x, depth=depth + 1) for x in obj]
    if isinstance(obj, str):
        if len(obj) > 4000:
            return obj[:4000] + "...[truncated]"
        if _ABS_PATH_RE.search(obj) and len(obj) < 500:
            return _ABS_PATH_RE.sub("<path>", obj)
        return obj
    if isinstance(obj, (int, float, bool)) or obj is None:
        return obj
    return str(obj)


def build_validation_snapshot(
    result_data: dict[str, Any],
    *,
    opera_panel: Optional[dict[str, Any]] = None,
    csv_payload: Any = None,
    json_payload: Any = None,
) -> dict[str, Any]:
    """Assemble a compact, audit-oriented snapshot from session ``result_data``."""
    pubchem = result_data.get("pubchem") or {}
    ghs = pubchem.get("ghs") or {}
    h_codes = list(ghs.get("h_codes") or [])
    p_codes = list(ghs.get("p_codes") or [])
    accounting = ghs_formatter.account_ghs_codes(h_codes, p_codes)
    qa_status = qa_checks.build_qa_status(
        result_data, csv_payload=csv_payload, json_payload=json_payload
    )
    toxval = result_data.get("toxval_data")
    prioritized = data_formatter.prioritize_toxicity_data(pubchem, toxval)
    eco = pubchem.get("ecotoxicity") or {}
    opera = opera_panel or {}
    opera_warnings = list(opera.get("warnings") or [])

    snapshot = {
        "snapshot_version": "v6_validation_1",
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "identifiers": {
            "cas": result_data.get("clean_cas"),
            "dtxsid": result_data.get("dtxsid")
            or (result_data.get("dsstox_info") or {}).get("dtxsid"),
            "preferred_name": result_data.get("preferred_name"),
            "pubchem_cid": pubchem.get("cid"),
            "iupac_name": pubchem.get("iupac_name") or pubchem.get("IUPACName"),
            "pubchem_title": pubchem.get("title") or pubchem.get("preferred_name"),
        },
        "retrieved_ghs": {
            "h_codes": h_codes,
            "p_codes": p_codes,
            "signal_word": ghs.get("signal_word"),
        },
        "ghs_accounting": accounting,
        "ecotoxicity": {
            "entries": eco.get("entries") or eco.get("endpoints") or [],
            "aquatic_ghs_codes": eco.get("h_codes_aquatic")
            or eco.get("aquatic_ghs_codes")
            or eco.get("aquatic_ghs")
            or [],
            "flags": eco.get("flags") or [],
            "aquatic_ghs_classification_only": eco.get("aquatic_ghs_classification_only"),
            "text_only_aquatic_classification": eco.get("text_only_aquatic_classification"),
        },
        "toxicity_prioritized": {
            "quantitative": prioritized.get("quantitative") or [],
            "categorical": prioritized.get("categorical") or [],
            "incomplete_excluded_count": len(prioritized.get("incomplete") or []),
        },
        "opera": {
            "available": bool(opera.get("available") or opera.get("predictions") or opera.get("row")),
            "warnings": opera_warnings,
            "selected_fields": {
                k: opera.get(k)
                for k in ("LogP_pred", "CAS", "MoleculeID", "SMILES", "error")
                if k in opera or (opera.get("predictions") or {}).get(k) is not None
            },
        },
        "qa": {
            "ok": qa_status.get("ok"),
            "warnings": qa_status.get("warnings") or [],
            "issues": qa_status.get("issues") or [],
            "retrieved_h_count": qa_status.get("retrieved_h_count"),
            "displayed_h_count": qa_status.get("displayed_h_count"),
            "unmapped_h_codes": qa_status.get("unmapped_h_codes"),
            "suppressed_or_subsumed_h_codes": qa_status.get("suppressed_or_subsumed_h_codes"),
        },
        "provenance": {
            "parser_version": pubchem.get("parser_version"),
            "retrieved_at": pubchem.get("retrieved_at"),
            "lexicon_version": accounting.get("lexicon_version"),
        },
    }
    return _scrub(snapshot)


def write_validation_snapshot(
    result_data: dict[str, Any],
    *,
    opera_panel: Optional[dict[str, Any]] = None,
    csv_payload: Any = None,
    json_payload: Any = None,
    out_dir: Optional[Path] = None,
    filename: Optional[str] = None,
) -> Path:
    """Write snapshot JSON; return path written."""
    cas = str(result_data.get("clean_cas") or "unknown").strip() or "unknown"
    safe_cas = cas.replace("/", "_")
    directory = Path(out_dir) if out_dir else default_snapshot_dir()
    directory.mkdir(parents=True, exist_ok=True)
    name = filename or f"{safe_cas}_v6_result_data.json"
    path = directory / name
    payload = build_validation_snapshot(
        result_data,
        opera_panel=opera_panel,
        csv_payload=csv_payload,
        json_payload=json_payload,
    )
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path
