#!/usr/bin/env python3
"""Build CAS-keyed P2OASys harvest SQLite from pages 1–101 (or compiled CSV).

Splits:
  ghhaz7        — clean single-CAS cell (exactly one CAS, nothing else)
  parked_noise  — one CAS token plus extra text
  parked_multi  — two or more CAS tokens (do not inherit onto one CAS)
  parked_empty  — no CAS token

Duplicate clean CAS: log conflicts; keep one preferred non-mixture row.
Does not invent scores. Score cells are numeric 0–10 on non-memo, non-measurement units.
"""
from __future__ import annotations

import csv
import json
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

import config  # noqa: E402

HAZQUERY = Path(config._HAZQUERY_ROOT)
FAST = Path(config.FASTP2OASYS_DIR)
PAGES_DIR = FAST / "p2oasys_site_exports"
COMPILED = FAST / "grok_workspace_keep" / "p2oasys_site_exports" / "P2OASys_harvest_compiled.csv"
if not COMPILED.is_file():
    COMPILED = PAGES_DIR / "P2OASys_harvest_compiled.csv"
OUT_DB = Path(
    getattr(config, "P2OASYS_HARVEST_DB", None)
    or (FAST / "p2oasys_harvest.sqlite")
)
LOOKUP_DB = Path(config.P2OASYS_SCORE_LOOKUP_DB)
MANIFEST_OUT = FAST / "p2oasys_harvest_manifest.json"

AUTO6 = [
    "Acute Human Effects",
    "Chronic Human Effects",
    "Ecological Hazards",
    "Environmental Fate & Transport",
    "Atmospheric Hazard",
    "Physical Properties",
]
HUMAN_ONLY = ["Process Factors", "Life Cycle Factors"]
ALL8 = AUTO6 + HUMAN_ONLY
CAT_TO_EXPERT_COL = {
    "Acute Human Effects": "expert_acute",
    "Chronic Human Effects": "expert_chronic",
    "Ecological Hazards": "expert_ecological",
    "Environmental Fate & Transport": "expert_fate",
    "Atmospheric Hazard": "expert_atmospheric",
    "Physical Properties": "expert_physical",
    "Process Factors": "expert_process",
    "Life Cycle Factors": "expert_life_cycle",
}

CAT_CANON = {
    "acute human effects": "Acute Human Effects",
    "chronic human effects": "Chronic Human Effects",
    "ecological hazards": "Ecological Hazards",
    "environmental fate & transport": "Environmental Fate & Transport",
    "environmental fate and transport": "Environmental Fate & Transport",
    "atmospheric hazard": "Atmospheric Hazard",
    "physical properties": "Physical Properties",
    "process factors": "Process Factors",
    "work environment process specific factors": "Process Factors",
    "work environment process specific factor": "Process Factors",
    "life cycle factors": "Life Cycle Factors",
}

CAS_TOKEN_RE = re.compile(r"\b(\d{2,7}-\d{2}-\d)\b")
PRODUCTISH = re.compile(
    r"\b(detergent|cleaner|remover|softener|handwash|laundry|foam|spray|"
    r"product|solution|blend|mix|formula|brand|system|steam|bath|"
    r"refrigerant|oxygenated)\b",
    re.I,
)
MIXTURE_NAME_RE = re.compile(
    r"MIXTURE\(S\)|SOLUTION\(S\)|WITH MORE THAN",
    re.I,
)
ALLOW_MIXED_ISOMERS = re.compile(r"\[MIXED ISOMERS\]", re.I)

MEASUREMENT_UNIT_RE = re.compile(
    r"("
    r"lc50|ld50|ec50|ecx|noec|noaec|chv|"
    r"mg/?\s*(kg|l|m3)|"
    r"\bppm\b|"
    r"mm\s*hg|"
    r"flash\s*point|"
    r"deg\s*c|"
    r"t1/2|"
    r"log\s*kow|log\s*pow|\bpow\b|baf/?bcf|"
    r"gwp\s*relative|"
    r"odp\s*units|"
    r"ph\s*units|"
    r"\bg/l\b|"
    r"wbgt|dba|"
    r"mm/s|"
    r"%|"
    r"days"
    r")",
    re.I,
)
SCORE_UNIT_HINT = re.compile(
    r"("
    r"key\s*phrase|key\s*word|"
    r"ghs|"
    r"nfpa|hmis|"
    r"category|classification|"
    r"\by/n\b|"
    r"hazard\s*level|"
    r"occur|"
    r"iarc|epa\s*class|acgih|osha|echa|prop\s*65|"
    r"tedx|\bsin\b|ospar|svhc|asthma|"
    r"endocrine|"
    r"\btype\b"
    r")",
    re.I,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS chemicals (
  col_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  cas_raw TEXT,
  split TEXT NOT NULL,
  cas TEXT,
  cas_digits TEXT,
  n_cas_tokens INTEGER NOT NULL DEFAULT 0,
  sds_source TEXT,
  sds_year TEXT,
  n_score_cells INTEGER,
  n_sub INTEGER,
  auto6_n INTEGER,
  chosen INTEGER NOT NULL DEFAULT 0,
  conflict INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cas_tokens (
  col_id INTEGER NOT NULL,
  cas TEXT NOT NULL,
  cas_digits TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS subcat_scores (
  col_id INTEGER NOT NULL,
  cas TEXT,
  source TEXT NOT NULL,
  category TEXT NOT NULL,
  subcategory TEXT NOT NULL,
  score REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_harvest_sub_cas ON subcat_scores(cas, source);
CREATE INDEX IF NOT EXISTS idx_harvest_chem_cas ON chemicals(cas_digits, chosen);
CREATE TABLE IF NOT EXISTS conflicts (
  cas TEXT,
  cas_digits TEXT,
  chosen_col_id INTEGER,
  chosen_name TEXT,
  rejected_col_id INTEGER,
  rejected_name TEXT,
  reason TEXT
);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def digits(cas: str) -> str:
    return re.sub(r"[^0-9]", "", cas or "")


def format_cas(d: str) -> str:
    if len(d) >= 5:
        return f"{d[:-3]}-{d[-3:-1]}-{d[-1]}"
    return d


def parse_score(raw: str) -> float | None:
    s = (raw or "").strip()
    if not s or not re.fullmatch(r"[-+]?\d+(?:\.\d+)?", s):
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    if 0.0 <= v <= 10.0:
        return v
    return None


def is_memo_unit(unit: str) -> bool:
    return (unit or "").strip().lower().startswith("memo")


def is_score_unit(unit: str) -> bool:
    u = (unit or "").strip()
    if not u or is_memo_unit(u):
        return False
    if SCORE_UNIT_HINT.search(u):
        return True
    if MEASUREMENT_UNIT_RE.search(u):
        return False
    return True


def canon_cat(raw: str) -> str | None:
    key = re.sub(r"\s+", " ", (raw or "").strip().lower())
    return CAT_CANON.get(key)


def classify_cas_cell(raw: str) -> tuple[str, list[str]]:
    cell = (raw or "").strip()
    tokens = CAS_TOKEN_RE.findall(cell)
    uniq: list[str] = []
    seen: set[str] = set()
    for t in tokens:
        k = digits(t)
        if k and k not in seen:
            seen.add(k)
            uniq.append(t)
    if not uniq:
        return "parked_empty", []
    if len(uniq) >= 2:
        return "parked_multi", uniq
    only = re.fullmatch(r"\s*\d{2,7}-\d{2}-\d\s*", cell)
    if only:
        return "ghhaz7", uniq
    return "parked_noise", uniq


def mixture_name(name: str) -> bool:
    if ALLOW_MIXED_ISOMERS.search(name or ""):
        return False
    return bool(MIXTURE_NAME_RE.search(name or ""))


def extract_column(rows: list[list[str]], ci: int) -> dict:
    def cell(ri: int) -> str:
        if ri >= len(rows) or ci >= len(rows[ri]):
            return ""
        return (rows[ri][ci] or "").strip()

    name = cell(0)
    cas_raw = sds_source = sds_year = ""
    for r in rows[:12]:
        label = (r[0] if r else "").strip().lower()
        val = r[ci].strip() if ci < len(r) else ""
        if label in {"cas #", "cas#", "cas"}:
            cas_raw = val
        elif label == "sds source":
            sds_source = val
        elif label == "sds year":
            sds_year = val

    sub_vals: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    n_score_cells = 0
    for r in rows:
        if not r:
            continue
        cat = canon_cat(r[0] if r else "")
        if cat is None:
            continue
        sub = (r[1] if len(r) > 1 else "").strip() or "(unspecified)"
        unit = (r[2] if len(r) > 2 else "").strip()
        val = r[ci].strip() if ci < len(r) else ""
        if not val or is_memo_unit(unit) or not is_score_unit(unit):
            continue
        score = parse_score(val)
        if score is None:
            continue
        sub_vals[cat][sub].append(score)
        n_score_cells += 1

    sub_max: dict[str, dict[str, float]] = {}
    cat_max: dict[str, float | None] = {}
    n_sub = 0
    auto6_n = 0
    for cat in ALL8:
        maxima = {s: max(vs) for s, vs in sub_vals.get(cat, {}).items() if vs}
        sub_max[cat] = maxima
        n_sub += len(maxima)
        cat_max[cat] = max(maxima.values()) if maxima else None
        if cat in AUTO6 and maxima:
            auto6_n += 1

    split, tokens = classify_cas_cell(cas_raw)
    return {
        "name": name,
        "cas_raw": cas_raw,
        "split": split,
        "tokens": tokens,
        "sds_source": sds_source,
        "sds_year": sds_year,
        "n_score_cells": n_score_cells,
        "n_sub": n_sub,
        "auto6_n": auto6_n,
        "sub_max": sub_max,
        "cat_max": cat_max,
    }


def rank_row(rec: dict) -> tuple:
    name = rec["name"] or ""
    return (
        rec["split"] == "ghhaz7",
        not mixture_name(name),
        not bool(PRODUCTISH.search(name)),
        rec["auto6_n"],
        rec["n_sub"],
        rec["n_score_cells"],
    )


def load_wide() -> list[list[str]]:
    if COMPILED.is_file():
        print(f"Reading compiled {COMPILED}", flush=True)
        with COMPILED.open(newline="", encoding="utf-8", errors="replace") as f:
            return list(csv.reader(f))
    pages = sorted(
        PAGES_DIR.glob("P2OASys_export_page*.csv"),
        key=lambda p: int(re.search(r"(\d+)", p.stem).group(1)),
    )
    if not pages:
        raise SystemExit(f"No harvest CSVs in {PAGES_DIR} or {COMPILED}")
    print(f"Merging {len(pages)} page files", flush=True)
    base = None
    for path in pages:
        with path.open(newline="", encoding="utf-8", errors="replace") as f:
            rows = list(csv.reader(f))
        if base is None:
            base = [r[:3] for r in rows]
            nrows = len(base)
        if len(rows) < nrows:
            rows = rows + [[""] * max(len(r) for r in rows)] * (nrows - len(rows))
        width = max(len(r) for r in rows)
        for ci in range(3, width):
            raw_name = (rows[0][ci] if len(rows[0]) > ci else "").strip()
            if not raw_name:
                continue
            for ri in range(nrows):
                val = rows[ri][ci] if len(rows[ri]) > ci else ""
                base[ri].append(val)
    return base


def build() -> dict:
    rows = load_wide()
    width = max(len(r) for r in rows)
    print(f"Wide table {len(rows)} rows x {width} cols", flush=True)

    recs: list[dict] = []
    for ci in range(3, width):
        rec = extract_column(rows, ci)
        rec["col_id"] = ci
        recs.append(rec)
        if (ci - 2) % 500 == 0:
            print(f"  extracted {ci - 2} chemicals...", flush=True)

    # Choose one GHaz7 row per clean CAS.
    by_cas: dict[str, list[dict]] = defaultdict(list)
    for rec in recs:
        if rec["split"] != "ghhaz7" or not rec["tokens"]:
            rec["chosen"] = 0
            rec["conflict"] = 0
            rec["cas"] = format_cas(digits(rec["tokens"][0])) if rec["tokens"] else None
            rec["cas_digits"] = digits(rec["tokens"][0]) if rec["tokens"] else None
            continue
        d = digits(rec["tokens"][0])
        rec["cas"] = format_cas(d)
        rec["cas_digits"] = d
        rec["chosen"] = 0
        rec["conflict"] = 0
        by_cas[d].append(rec)

    conflicts: list[dict] = []
    for d, group in by_cas.items():
        ranked = sorted(group, key=rank_row, reverse=True)
        winner = ranked[0]
        winner["chosen"] = 1
        for loser in ranked[1:]:
            loser["chosen"] = 0
            loser["conflict"] = 1
            loser["split"] = "parked_dup_cas"
            conflicts.append(
                {
                    "cas": winner["cas"],
                    "cas_digits": d,
                    "chosen_col_id": winner["col_id"],
                    "chosen_name": winner["name"],
                    "rejected_col_id": loser["col_id"],
                    "rejected_name": loser["name"],
                    "reason": "duplicate_clean_cas; kept higher rank (non-mixture, fill)",
                }
            )

    OUT_DB.parent.mkdir(parents=True, exist_ok=True)
    if OUT_DB.is_file():
        OUT_DB.unlink()
    conn = sqlite3.connect(str(OUT_DB))
    conn.executescript(SCHEMA)
    chem_rows = []
    tok_rows = []
    sub_rows = []
    for rec in recs:
        cas = rec.get("cas")
        chosen = int(rec.get("chosen") or 0)
        chem_rows.append(
            (
                rec["col_id"],
                rec["name"],
                rec["cas_raw"],
                rec["split"],
                cas,
                rec.get("cas_digits"),
                len(rec["tokens"]),
                rec["sds_source"],
                rec["sds_year"],
                rec["n_score_cells"],
                rec["n_sub"],
                rec["auto6_n"],
                chosen,
                int(rec.get("conflict") or 0),
            )
        )
        for t in rec["tokens"]:
            tok_rows.append((rec["col_id"], t, digits(t)))
        store_cas = cas if chosen else None
        for cat, maxima in rec["sub_max"].items():
            for sub, score in maxima.items():
                sub_rows.append((rec["col_id"], store_cas, "expert", cat, sub, float(score)))

    conn.executemany(
        "INSERT INTO chemicals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        chem_rows,
    )
    conn.executemany("INSERT INTO cas_tokens VALUES (?,?,?)", tok_rows)
    conn.executemany(
        "INSERT INTO subcat_scores VALUES (?,?,?,?,?,?)",
        sub_rows,
    )
    conn.executemany(
        "INSERT INTO conflicts VALUES (?,?,?,?,?,?,?)",
        [
            (
                c["cas"],
                c["cas_digits"],
                c["chosen_col_id"],
                c["chosen_name"],
                c["rejected_col_id"],
                c["rejected_name"],
                c["reason"],
            )
            for c in conflicts
        ],
    )
    counts = {
        "n_columns": len(recs),
        "n_ghhaz7_chosen": sum(1 for r in recs if r.get("chosen")),
        "n_ghhaz7_rows_before_collapse": sum(1 for r in recs if r["split"] == "ghhaz7" or r["split"] == "parked_dup_cas"),
        "n_parked_multi": sum(1 for r in recs if r["split"] == "parked_multi"),
        "n_parked_empty": sum(1 for r in recs if r["split"] == "parked_empty"),
        "n_parked_noise": sum(1 for r in recs if r["split"] == "parked_noise"),
        "n_dup_conflicts": len(conflicts),
        "n_expert_subcat_cells_chosen": sum(
            1 for r in recs if r.get("chosen") for cat in r["sub_max"].values() for _ in cat
        ),
        "compiled": str(COMPILED) if COMPILED.is_file() else None,
        "built_at": now_iso(),
    }
    # unique CAS tokens overall
    all_tok = {digits(t) for r in recs for t in r["tokens"]}
    counts["n_unique_cas_tokens"] = len(all_tok)
    for k, v in counts.items():
        conn.execute("INSERT INTO meta(k,v) VALUES (?,?)", (k, json.dumps(v)))
    conn.commit()
    conn.close()
    MANIFEST_OUT.write_text(json.dumps(counts, indent=2), encoding="utf-8")
    print(json.dumps(counts, indent=2), flush=True)
    print(f"Wrote {OUT_DB}", flush=True)
    return counts


def seed_lookup(counts: dict) -> None:
    from utils import p2oasys_score_lookup

    print(f"Seeding GHaz7 lookup {LOOKUP_DB}", flush=True)
    n = p2oasys_score_lookup.seed_expert_from_harvest(
        harvest_db=OUT_DB, db_path=LOOKUP_DB, force=True
    )
    print(f"Lookup expert subcat cells written: {n}", flush=True)
    counts["lookup_expert_subcat_cells"] = n
    MANIFEST_OUT.write_text(json.dumps(counts, indent=2), encoding="utf-8")


def main() -> int:
    counts = build()
    seed_lookup(counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
