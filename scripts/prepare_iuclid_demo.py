#!/usr/bin/env python3
"""
Prepare committed IUCLID demo assets for Streamlit Cloud (run on your machine).

- Optionally zip N .i6z files from a local REACH extraction into data/reach_demo/reach_subset.zip
- Optionally copy the IUCLID format tree into data/iuclid_format/IUCLID_6_9_0_0_format (no spaces)

Does not read paths outside what you pass on the CLI. See data/echa_cloud/README.txt for context.
"""

from __future__ import annotations

import argparse
import shutil
import zipfile
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def copy_format_tree(src: Path, dest: Path, dry_run: bool) -> None:
    if not src.is_dir():
        raise SystemExit(f"Format source is not a directory: {src}")
    if dest.exists() and any(dest.iterdir()):
        print(f"Destination already has content, skipping copy: {dest}")
        return
    print(f"Copying format tree:\n  {src}\n  -> {dest}")
    if not dry_run:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dest, dirs_exist_ok=True)


def zip_i6z_subset(source_dir: Path, out_zip: Path, limit: int, dry_run: bool) -> None:
    if not source_dir.is_dir():
        raise SystemExit(f"Source dossier folder not found: {source_dir}")
    i6z = sorted(source_dir.rglob("*.i6z"))
    if not i6z:
        raise SystemExit(f"No .i6z files under {source_dir}")
    pick = i6z[:limit]
    print(f"Zipping {len(pick)} of {len(i6z)} .i6z files into {out_zip}")
    if dry_run:
        for p in pick:
            print("  ", p)
        return
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in pick:
            arc = p.name
            zf.write(p, arcname=arc)
    print(f"Wrote {out_zip} ({out_zip.stat().st_size // 1024} KiB)")


def main() -> None:
    root = _repo_root()
    p = argparse.ArgumentParser(description="Prepare IUCLID format + REACH demo zip under data/")
    p.add_argument(
        "--format-src",
        type=Path,
        help="Path to extracted IUCLID format folder (e.g. .../IUCLID 6 9.0.0_format)",
    )
    p.add_argument(
        "--i6z-dir",
        type=Path,
        help="Folder containing .i6z dossiers (e.g. extracted REACH zip)",
    )
    p.add_argument("--limit", type=int, default=8, help="Max .i6z files to include in demo zip (default 8)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    dest_format = root / "data" / "iuclid_format" / "IUCLID_6_9_0_0_format"
    dest_zip = root / "data" / "reach_demo" / "reach_subset.zip"

    if args.format_src:
        copy_format_tree(args.format_src.resolve(), dest_format, args.dry_run)
    if args.i6z_dir:
        zip_i6z_subset(args.i6z_dir.resolve(), dest_zip, args.limit, args.dry_run)

    print(
        "\nNext: git add data/iuclid_format data/reach_demo data/echa_cloud\n"
        "Then commit (watch GitHub file size). On Streamlit Cloud, Secrets can stay empty if defaults apply.\n"
        "\n"
        "Reminder: reach_subset.zip is a DEMO subset — not full REACH; document this in README / "
        "data/reach_demo/README.md for anyone cloning the repo."
    )


if __name__ == "__main__":
    main()
