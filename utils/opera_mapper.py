"""
Map OPERA endpoints to likely experimental ToxVal study types.

This module keeps mapping logic in one place so cross-validation and UI
components can share a consistent endpoint->experimental lookup.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from utils import opera_batch

_LOG = logging.getLogger(__name__)


DEFAULT_HINTS: dict[str, tuple[str, ...]] = {
    "LogP_pred": ("logp", "log p", "kow", "octanol"),
    "LogWS_pred": ("solubility", "water solubility", "aqueous"),
    "LogBCF_pred": ("bioconcentration", "bcf"),
    "BioDeg_LogHalfLife_pred": ("half-life", "half life", "biodeg"),
    "CATMoS_LD50_pred": ("ld50", "oral", "acute", "lethal dose"),
    "MP_pred": ("melting point",),
    "BP_pred": ("boiling point",),
    "LogKoc_pred": ("koc", "soil adsorption", "organic carbon"),
    "LogHL_pred": ("henry", "air-water"),
    "LogVP_pred": ("vapor pressure", "vapour pressure"),
}

MIN_TYPE_COUNT = 5


def _normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("/", " ").replace("-", " ").replace(";", " ")
    s = re.sub(r"\s+", " ", s)
    return s


@dataclass
class OperaEndpointMapper:
    """
    Build endpoint mappings against local ToxVal records.

    Supports ToxVal in either:
    - local SQLite (`toxvaldb` table), or
    - CSV with at least a type column (e.g. `study_type` / `toxval_type`).
    """

    endpoints_json_path: Path
    toxval_sqlite_path: Path | None = None
    toxval_csv_path: Path | None = None
    mapping_output_path: Path | None = None

    def load_opera_endpoints(self) -> list[str]:
        meta = opera_batch.get_all_opera_endpoints(self.endpoints_json_path)
        return [m["name"] for m in meta if m.get("name")]

    def _load_toxval_types_sqlite(self) -> pd.DataFrame:
        if not self.toxval_sqlite_path or not self.toxval_sqlite_path.is_file():
            return pd.DataFrame(columns=["toxval_type", "n_rows"])
        con = sqlite3.connect(self.toxval_sqlite_path)
        try:
            q = """
                SELECT LOWER(TRIM(COALESCE(study_type, ''))) AS toxval_type, COUNT(*) AS n_rows
                FROM toxvaldb
                WHERE COALESCE(study_type, '') != ''
                GROUP BY LOWER(TRIM(COALESCE(study_type, '')))
                ORDER BY COUNT(*) DESC
            """
            return pd.read_sql_query(q, con)
        finally:
            con.close()

    @staticmethod
    def _pick_type_col(df: pd.DataFrame) -> str | None:
        candidates = ("study_type", "toxval_type", "endpoint", "type")
        cols_l = {c.lower(): c for c in df.columns}
        for c in candidates:
            if c in cols_l:
                return cols_l[c]
        return None

    def _load_toxval_types_csv(self) -> pd.DataFrame:
        if not self.toxval_csv_path or not self.toxval_csv_path.is_file():
            return pd.DataFrame(columns=["toxval_type", "n_rows"])
        df = pd.read_csv(self.toxval_csv_path, dtype=str, low_memory=False)
        type_col = self._pick_type_col(df)
        if not type_col:
            return pd.DataFrame(columns=["toxval_type", "n_rows"])
        ser = (
            df[type_col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )
        ser = ser[ser != ""]
        vc = ser.value_counts(dropna=True).rename_axis("toxval_type").reset_index(name="n_rows")
        return vc

    def load_toxval_types(self) -> pd.DataFrame:
        """Profile unique ToxVal endpoint/type labels with counts."""
        if self.toxval_sqlite_path and self.toxval_sqlite_path.is_file():
            return self._load_toxval_types_sqlite()
        return self._load_toxval_types_csv()

    @staticmethod
    def _guess_keywords(endpoint: str) -> tuple[str, ...]:
        if endpoint in DEFAULT_HINTS:
            return DEFAULT_HINTS[endpoint]
        ep = endpoint.lower()
        if "logp" in ep or "logd" in ep:
            return ("logp", "log p", "kow", "partition")
        if "ld50" in ep or "catmos" in ep:
            return ("ld50", "acute", "oral")
        if "bcf" in ep:
            return ("bioconcentration", "bcf")
        if "solubility" in ep or "ws" in ep:
            return ("solubility",)
        return (endpoint.replace("_pred", "").replace("_", " ").lower(),)

    @staticmethod
    def _tox_domain_keywords(endpoint: str) -> tuple[str, ...]:
        ep = endpoint.lower()
        if "catmos" in ep or "ld50" in ep:
            return ("acute", "oral", "lethal", "toxicity")
        if "cerapp" in ep or "compara" in ep:
            return ("reproductive", "developmental", "uterotrophic", "hershberger", "in vitro")
        if "fub" in ep or "clint" in ep or "caco2" in ep:
            return ("in vitro", "clinical", "occupational")
        if "biodeg" in ep:
            return ("chronic", "subchronic", "short term")
        return ()

    @staticmethod
    def _is_relevant_type(endpoint: str) -> bool:
        ep = endpoint.lower()
        return (
            "catmos" in ep
            or "cerapp" in ep
            or "compara" in ep
            or "fub" in ep
            or "clint" in ep
            or "caco2" in ep
            or "biodeg" in ep
        )

    def build_mapping(self) -> dict[str, Any]:
        endpoints = self.load_opera_endpoints()
        tox_types = self.load_toxval_types()
        tox_type_vals = [_normalize_text(t) for t in tox_types["toxval_type"].astype(str).tolist()] if not tox_types.empty else []

        by_type_count = {
            _normalize_text(str(r["toxval_type"])): int(r["n_rows"])
            for _, r in tox_types.iterrows()
        } if not tox_types.empty else {}

        mapping: dict[str, Any] = {
            "meta": {
                "n_opera_endpoints": len(endpoints),
                "n_toxval_types_profiled": len(tox_type_vals),
            },
            "mapping": {},
        }
        total_rows = sum(by_type_count.values())
        min_count = MIN_TYPE_COUNT if total_rows >= 500 else 1
        for ep in endpoints:
            kws = tuple(_normalize_text(k) for k in self._guess_keywords(ep))
            domain_kws = tuple(_normalize_text(k) for k in self._tox_domain_keywords(ep))
            if self._is_relevant_type(ep):
                matches = [
                    t
                    for t in tox_type_vals
                    if by_type_count.get(t, 0) >= min_count
                    and (any(k in t for k in kws) or any(k in t for k in domain_kws))
                ]
            else:
                # Keep physchem endpoint mapping strict to avoid false positives from generic
                # toxicity-only study types in local ToxVal snapshots.
                matches = [
                    t
                    for t in tox_type_vals
                    if by_type_count.get(t, 0) >= min_count and any(k in t for k in kws)
                ]
            mapping["mapping"][ep] = {
                "keywords": list(kws),
                "domain_keywords": list(domain_kws),
                "toxval_types": matches,
                "toxval_type_counts": {m: by_type_count.get(m, 0) for m in matches},
            }
        mapping["meta"]["min_count_filter"] = min_count
        return mapping

    def save_mapping(self, payload: dict[str, Any]) -> Path:
        out = self.mapping_output_path or (self.endpoints_json_path.parent / "opera_to_toxval_mapping.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return out

    def build_and_save(self) -> Path:
        payload = self.build_mapping()
        out = self.save_mapping(payload)
        _LOG.info("Saved OPERA->ToxVal mapping to %s", out)
        return out


def load_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"meta": {}, "mapping": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"meta": {}, "mapping": {}}
