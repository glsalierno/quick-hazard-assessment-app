#!/usr/bin/env python3
"""
SDS parsing accuracy / agreement report.

Without a human-labeled gold set, this script uses the **unified SDS parser**
(regex + engine + Docling merge) as the **reference** and compares:

  - **pure_bert**: Docling + DistilBERT CAS path (``utils.cas_extractor``)
  - **docling_only**: Docling composition tables only (``docling_sds_parser``)

Metrics are **set overlap** per PDF (CAS numbers as normalized strings):
precision / recall / F1 for pure_bert vs reference, and docling_only vs reference.

**Progress:** writes one CSV row per PDF (append + flush), a **progress** text file,
and a **checkpoint** JSON so you can ``--resume`` after interruption.

**Memory:** use ``--low-memory`` (default on) for Docling without OCR + FAST tables;
tune ``--threads`` to cap CPU thread explosion on Windows.

Usage (from ``quick-hazard-assessment-app``)::

  python scripts/sds_parsing_accuracy_report.py --folder "../sds examples" --out-dir artifacts

  python scripts/sds_parsing_accuracy_report.py --folder "../sds examples" --resume

  python scripts/sds_parsing_accuracy_report.py --folder "../sds examples" --no-low-memory
"""

from __future__ import annotations

import gc
import os
import warnings

# Quiet CLI noise when SDS modules import Streamlit / torch (no app context).
os.environ.setdefault("STREAMLIT_LOGGER_LEVEL", "error")
warnings.filterwarnings("ignore", message=".*ScriptRunContext.*")
warnings.filterwarnings("ignore", message=".*Session state does not function.*")
warnings.filterwarnings("ignore", message=".*pin_memory.*")
warnings.filterwarnings("ignore", message=".*torch_dtype.*")

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO


def _norm_cas(c: str) -> str:
    from utils import cas_validator

    s = (c or "").strip()
    if not s:
        return ""
    return cas_validator.normalize_cas_input(s) or s


def _cas_set_from_strings(items: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for x in items:
        n = _norm_cas(x)
        if n:
            out.add(n)
    return out


def _ref_from_unified(pdf_bytes: bytes) -> set[str]:
    from utils.sds_parser import get_sds_parser

    r = get_sds_parser().parse_pdf(pdf_bytes)
    if not r or not r.cas_numbers:
        return set()
    return _cas_set_from_strings(e.cas for e in r.cas_numbers if e.cas)


def _cand_pure_bert(ext: Any, pdf_bytes: bytes) -> set[str]:
    rows = ext.extract(pdf_bytes)
    return _cas_set_from_strings(r.cas for r in rows)


def _cand_docling_only(pdf_bytes: bytes, *, low_memory: bool) -> set[str]:
    from utils import docling_sds_parser

    rows = docling_sds_parser.extract_composition_from_pdf(
        pdf_bytes, use_cache=False, low_memory=low_memory
    )
    if not rows:
        return set()
    return _cas_set_from_strings(x.cas for x in rows if x.cas)


def _apply_thread_cap(n: int) -> None:
    if n <= 0:
        return
    os.environ.setdefault("OMP_NUM_THREADS", str(n))
    os.environ.setdefault("MKL_NUM_THREADS", str(n))
    os.environ.setdefault("OPENBLAS_NUM_THREADS", str(n))
    os.environ.setdefault("NUMEXPR_NUM_THREADS", str(n))
    try:
        import torch

        torch.set_num_threads(n)
        try:
            torch.set_num_interop_threads(min(2, n))
        except Exception:
            pass
    except Exception:
        pass


def _release_docling_batch_heap() -> None:
    try:
        from utils.docling_sds_parser import reset_batch_low_memory_converter

        reset_batch_low_memory_converter()
    except Exception:
        pass


def _post_pdf_cleanup() -> None:
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:
        pass
    gc.collect()


@dataclass
class RowMetrics:
    filename: str
    seconds: float
    n_ref: int
    n_bert: int
    n_docling: int
    tp_bert: int
    fp_bert: int
    fn_bert: int
    tp_doc: int
    fp_doc: int
    fn_doc: int
    err: str


def _prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) > 0 else 0.0
    return prec, rec, f1


CSV_HEADER = [
    "filename",
    "seconds",
    "n_ref_unified",
    "n_pure_bert",
    "n_docling_only",
    "tp_bert",
    "fp_bert",
    "fn_bert",
    "prec_bert",
    "rec_bert",
    "f1_bert",
    "tp_docling",
    "fp_docling",
    "fn_docling",
    "prec_docling",
    "rec_docling",
    "f1_docling",
    "error",
]


def _load_completed_filenames(csv_path: Path) -> set[str]:
    done: set[str] = set()
    if not csv_path.is_file():
        return done
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "filename" not in reader.fieldnames:
            return done
        for row in reader:
            fn = row.get("filename") or ""
            if fn:
                done.add(fn)
    return done


def _write_progress(
    path: Path,
    *,
    global_idx: int,
    total: int,
    session_i: int,
    n_pending: int,
    filename: str,
    elapsed_session_s: float,
    row: RowMetrics,
    t_batch_start: float,
) -> None:
    eta = ""
    if session_i > 0 and n_pending > 0:
        rate = elapsed_session_s / session_i
        remain = max(0, n_pending - session_i)
        eta = f" | ETA_pending_s~{rate * remain:.0f}"
    wall = time.perf_counter() - t_batch_start
    line = (
        f"[{global_idx}/{total}] {filename} | "
        f"this_pdf_s={row.seconds:.1f} | wall_s={wall:.0f} | "
        f"ref={row.n_ref} bert={row.n_bert} doc={row.n_docling} | "
        f"err={'Y' if row.err else 'N'}{eta}\n"
    )
    path.write_text(line, encoding="utf-8")


def _write_checkpoint(path: Path, *, folder: str, completed: list[str], total: int) -> None:
    payload = {
        "folder": folder,
        "total_pdfs": total,
        "completed_count": len(completed),
        "completed_filenames": sorted(completed),
        "updated_utc": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_summary_artifacts(
    *,
    rows: list[RowMetrics],
    folder: Path,
    out_dir: Path,
    wall_s: float,
    low_memory: bool,
) -> dict[str, Any]:
    csv_path = out_dir / "sds_parsing_accuracy_report.csv"
    md_path = out_dir / "sds_parsing_accuracy_report.md"
    json_path = out_dir / "sds_parsing_accuracy_summary.json"

    sum_tp_b = sum(r.tp_bert for r in rows)
    sum_fp_b = sum(r.fp_bert for r in rows)
    sum_fn_b = sum(r.fn_bert for r in rows)
    sum_tp_d = sum(r.tp_doc for r in rows)
    sum_fp_d = sum(r.fp_doc for r in rows)
    sum_fn_d = sum(r.fn_doc for r in rows)

    p_b, r_b, f1_b = _prf(sum_tp_b, sum_fp_b, sum_fn_b)
    p_d, r_d, f1_d = _prf(sum_tp_d, sum_fp_d, sum_fn_d)

    def macro_f1(getter: str) -> float:
        fs: list[float] = []
        for r in rows:
            if r.n_ref == 0:
                continue
            if getter == "bert":
                tp, fp, fn = r.tp_bert, r.fp_bert, r.fn_bert
            else:
                tp, fp, fn = r.tp_doc, r.fp_doc, r.fn_doc
            _, _, f1 = _prf(tp, fp, fn)
            fs.append(f1)
        return sum(fs) / len(fs) if fs else 0.0

    macro_b = macro_f1("bert")
    macro_d = macro_f1("docling")

    summary = {
        "reference": "unified_sds_parser (regex + engine + docling merge)",
        "n_pdfs": len(rows),
        "wall_seconds": round(wall_s, 2),
        "low_memory_docling": low_memory,
        "pure_bert_vs_ref": {
            "micro_precision": round(p_b, 4),
            "micro_recall": round(r_b, 4),
            "micro_f1": round(f1_b, 4),
            "macro_f1_ref_nonempty": round(macro_b, 4),
            "totals": {"tp": sum_tp_b, "fp": sum_fp_b, "fn": sum_fn_b},
        },
        "docling_only_vs_ref": {
            "micro_precision": round(p_d, 4),
            "micro_recall": round(r_d, 4),
            "micro_f1": round(f1_d, 4),
            "macro_f1_ref_nonempty": round(macro_d, 4),
            "totals": {"tp": sum_tp_d, "fp": sum_fp_d, "fn": sum_fn_d},
        },
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# SDS parsing agreement report\n\n")
        f.write(
            "**Reference (proxy):** unified `SDSParser` — text/regex pipeline merged with Docling composition when available. "
            "This is *not* human ground truth; use it to compare **pure Docling+DistilBERT** vs the shipped default.\n\n"
        )
        f.write(f"- **PDFs evaluated:** {len(rows)}\n")
        f.write(f"- **Folder:** `{folder}`\n")
        f.write(f"- **Low-memory Docling batch (no OCR, FAST tables):** {low_memory}\n")
        f.write(f"- **Total wall time:** {summary['wall_seconds']} s\n\n")

        f.write("## Pure BERT (`utils.cas_extractor`) vs reference\n\n")
        f.write("| Metric | Value |\n|--------|-------|\n")
        f.write(f"| Micro precision | {p_b:.4f} |\n")
        f.write(f"| Micro recall | {r_b:.4f} |\n")
        f.write(f"| Micro F1 | {f1_b:.4f} |\n")
        f.write(f"| Macro F1 (files where ref has ≥1 CAS) | {macro_b:.4f} |\n")
        f.write(f"| TP / FP / FN (pooled) | {sum_tp_b} / {sum_fp_b} / {sum_fn_b} |\n\n")

        f.write("## Docling-only composition vs reference\n\n")
        f.write("| Metric | Value |\n|--------|-------|\n")
        f.write(f"| Micro precision | {p_d:.4f} |\n")
        f.write(f"| Micro recall | {r_d:.4f} |\n")
        f.write(f"| Micro F1 | {f1_d:.4f} |\n")
        f.write(f"| Macro F1 (files where ref has ≥1 CAS) | {macro_d:.4f} |\n")
        f.write(f"| TP / FP / FN (pooled) | {sum_tp_d} / {sum_fp_d} / {sum_fn_d} |\n\n")

        f.write("## Files\n\n")
        f.write(f"Per-file metrics (incremental): `{csv_path.name}`\n")

    print("Wrote:", csv_path)
    print("Wrote:", md_path)
    print("Wrote:", json_path)
    return summary


# forward ref for type hints
from typing import Any  # noqa: E402


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))

    ap = argparse.ArgumentParser()
    ap.add_argument("--folder", type=Path, default=root.parent / "sds examples")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--out-dir", type=Path, default=root / "artifacts")
    ap.add_argument(
        "--resume",
        action="store_true",
        help="Skip PDFs already listed in the output CSV; append new rows only.",
    )
    ap.add_argument(
        "--no-low-memory",
        action="store_true",
        help="Use full Docling (OCR on, accurate tables). Uses much more RAM.",
    )
    ap.add_argument(
        "--threads",
        type=int,
        default=2,
        help="Cap OMP/MKL/torch CPU threads (lower reduces RAM spikes; default 2).",
    )
    args = ap.parse_args()

    low_memory = not args.no_low_memory
    if low_memory:
        os.environ.setdefault("HAZQUERY_DOCLING_LOW_MEMORY", "1")
    else:
        os.environ.pop("HAZQUERY_DOCLING_LOW_MEMORY", None)

    _apply_thread_cap(args.threads)

    folder = args.folder.resolve()
    if not folder.is_dir():
        print("Not a folder:", folder)
        return 1

    pdfs = sorted(folder.glob("*.pdf")) + sorted(folder.glob("*.PDF"))
    if args.limit:
        pdfs = pdfs[: args.limit]
    if not pdfs:
        print("No PDFs in", folder)
        return 1

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "sds_parsing_accuracy_report.csv"
    checkpoint_path = out_dir / "sds_parsing_accuracy_checkpoint.json"
    progress_path = out_dir / "sds_parsing_accuracy_progress.txt"

    completed = _load_completed_filenames(csv_path) if args.resume else set()
    pending = [p for p in pdfs if p.name not in completed]
    total = len(pdfs)
    n_already = len(completed)
    n_pending = len(pending)

    from utils.cas_extractor import MemoryOptimizedPureCASExtractor

    bert_ext = MemoryOptimizedPureCASExtractor(
        use_streamlit_converter_cache=False,
        low_memory_docling=low_memory,
    )

    all_rows: list[RowMetrics] = []
    if args.resume and csv_path.is_file():
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("filename"):
                    continue
                try:
                    all_rows.append(
                        RowMetrics(
                            filename=row["filename"],
                            seconds=float(row.get("seconds") or 0),
                            n_ref=int(row.get("n_ref_unified") or 0),
                            n_bert=int(row.get("n_pure_bert") or 0),
                            n_docling=int(row.get("n_docling_only") or 0),
                            tp_bert=int(row.get("tp_bert") or 0),
                            fp_bert=int(row.get("fp_bert") or 0),
                            fn_bert=int(row.get("fn_bert") or 0),
                            tp_doc=int(row.get("tp_docling") or 0),
                            fp_doc=int(row.get("fp_docling") or 0),
                            fn_doc=int(row.get("fn_docling") or 0),
                            err=row.get("error") or "",
                        )
                    )
                except (TypeError, ValueError):
                    continue

    append_mode = bool(completed) and csv_path.is_file()
    csv_f: TextIO
    csv_f = open(csv_path, "a" if append_mode else "w", newline="", encoding="utf-8")
    w = csv.writer(csv_f)
    if not append_mode:
        w.writerow(CSV_HEADER)
    csv_f.flush()

    t_batch = time.perf_counter()
    done_names = {r.filename for r in all_rows}

    try:
        for i, pdf in enumerate(pending, start=1):
            global_idx = n_already + i
            t0 = time.perf_counter()
            err = ""
            ref: set[str] = set()
            bert: set[str] = set()
            doc: set[str] = set()
            data: bytes = b""
            try:
                data = pdf.read_bytes()
                ref = _ref_from_unified(data)
                bert = _cand_pure_bert(bert_ext, data)
                doc = _cand_docling_only(data, low_memory=low_memory)
            except MemoryError as e:
                err = repr(e)
                _release_docling_batch_heap()
            except Exception as e:
                err = repr(e)
            finally:
                del data
                _post_pdf_cleanup()

            tp_b = len(ref & bert)
            fp_b = len(bert - ref)
            fn_b = len(ref - bert)
            tp_d = len(ref & doc)
            fp_d = len(doc - ref)
            fn_d = len(ref - doc)

            row = RowMetrics(
                filename=pdf.name,
                seconds=time.perf_counter() - t0,
                n_ref=len(ref),
                n_bert=len(bert),
                n_docling=len(doc),
                tp_bert=tp_b,
                fp_bert=fp_b,
                fn_bert=fn_b,
                tp_doc=tp_d,
                fp_doc=fp_d,
                fn_doc=fn_d,
                err=err,
            )
            all_rows.append(row)
            done_names.add(pdf.name)

            pb, rb, fb = _prf(row.tp_bert, row.fp_bert, row.fn_bert)
            pd_, rd, fd = _prf(row.tp_doc, row.fp_doc, row.fn_doc)
            w.writerow(
                [
                    row.filename,
                    f"{row.seconds:.2f}",
                    row.n_ref,
                    row.n_bert,
                    row.n_docling,
                    row.tp_bert,
                    row.fp_bert,
                    row.fn_bert,
                    f"{pb:.4f}",
                    f"{rb:.4f}",
                    f"{fb:.4f}",
                    row.tp_doc,
                    row.fp_doc,
                    row.fn_doc,
                    f"{pd_:.4f}",
                    f"{rd:.4f}",
                    f"{fd:.4f}",
                    row.err,
                ]
            )
            csv_f.flush()

            _write_progress(
                progress_path,
                global_idx=global_idx,
                total=total,
                session_i=i,
                n_pending=n_pending,
                filename=pdf.name,
                elapsed_session_s=time.perf_counter() - t_batch,
                row=row,
                t_batch_start=t_batch,
            )
            _write_checkpoint(
                checkpoint_path,
                folder=str(folder),
                completed=sorted(done_names),
                total=total,
            )

            print(
                f"[{global_idx}/{total}] {pdf.name}  "
                f"ref={row.n_ref} bert={row.n_bert} doc={row.n_docling}  {row.seconds:.1f}s",
                flush=True,
            )
    finally:
        csv_f.close()

    wall_s = time.perf_counter() - t_batch
    summary = _write_summary_artifacts(
        rows=all_rows,
        folder=folder,
        out_dir=out_dir,
        wall_s=wall_s,
        low_memory=low_memory,
    )
    print(json.dumps(summary, indent=2))
    _release_docling_batch_heap()
    _post_pdf_cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
