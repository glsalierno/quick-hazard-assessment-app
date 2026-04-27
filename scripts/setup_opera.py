#!/usr/bin/env python3
"""
Automate local OPERA setup for Quick Hazard Assessment.

What this script does:
1) Checks whether OPERA already exists at configured/common locations.
2) Queries GitHub releases for NIEHS/OPERA and picks a host-appropriate asset.
3) Downloads the release asset to a local install directory (no admin required).
4) Tries to locate OPERA.exe (or Linux binary) and updates `.env` with HAZQUERY_OPERA_EXE.

Notes:
- Windows silent installer behavior differs by release packaging. This script prefers
  download + extraction/copy and falls back to manual steps when unattended install
  cannot be guaranteed.
- The app only requires HAZQUERY_OPERA_EXE to point to a runnable executable.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from tarfile import open as tar_open
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = REPO_ROOT / ".env"
DEFAULT_OPERA_DIR = REPO_ROOT / "opera"
GITHUB_API_RELEASES = "https://api.github.com/repos/NIEHS/OPERA/releases"
RELEASES_PAGE = "https://github.com/NIEHS/OPERA/releases"


def _is_windows() -> bool:
    return platform.system().lower().startswith("win")


def _is_linux() -> bool:
    return platform.system().lower() == "linux"


def _request_json(url: str) -> Any | None:
    req = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "quick-hazard-assessment-opera-setup",
        },
    )
    try:
        with urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        print(f"[warn] GitHub API HTTP error: {exc.code} ({exc.reason})")
        return None
    except URLError as exc:
        print(f"[warn] GitHub API network error: {exc.reason}")
        return None


def _download_file(url: str, dst: Path) -> None:
    req = Request(
        url,
        headers={"User-Agent": "quick-hazard-assessment-opera-setup"},
    )
    with urlopen(req, timeout=120) as resp, open(dst, "wb") as fh:
        shutil.copyfileobj(resp, fh)


def _asset_score(name: str) -> int:
    n = name.lower()
    score = 0
    if _is_windows():
        if "win" in n:
            score += 20
        if n.endswith(".zip"):
            score += 10
        if n.endswith(".msi") or n.endswith(".exe"):
            score += 8
    elif _is_linux():
        if "linux" in n:
            score += 20
        if n.endswith(".tar.xz") or n.endswith(".tgz") or n.endswith(".tar.gz"):
            score += 10
    else:
        # macOS fallback: use Linux artifact if no native release exists.
        if "linux" in n:
            score += 8
    if "ui" in n:
        score += 1
    if "cl" in n:
        score += 2
    return score


def _pick_asset(release: dict[str, Any]) -> dict[str, Any] | None:
    assets = release.get("assets") or []
    ranked = sorted(assets, key=lambda a: _asset_score(str(a.get("name", ""))), reverse=True)
    for asset in ranked:
        if _asset_score(str(asset.get("name", ""))) > 0:
            return asset
    return None


def _discover_existing_opera() -> Path | None:
    candidates = []
    env_exe = (os.environ.get("HAZQUERY_OPERA_EXE") or os.environ.get("OPERA_EXE") or "").strip()
    if env_exe:
        candidates.append(Path(env_exe))

    if _is_windows():
        candidates.extend(
            [
                Path(r"C:\Program Files\OPERA\application\OPERA.exe"),
                Path(r"C:\Program Files\OPERA\application\OPERA_P.exe"),
                Path(r"C:\Program Files\OPERA2.9_CL\application\OPERA.exe"),
                DEFAULT_OPERA_DIR / "application" / "OPERA.exe",
                DEFAULT_OPERA_DIR / "OPERA.exe",
            ]
        )
    else:
        candidates.extend(
            [
                Path("/opt/opera/application/OPERA"),
                Path("/usr/local/bin/OPERA"),
                DEFAULT_OPERA_DIR / "application" / "OPERA",
                DEFAULT_OPERA_DIR / "OPERA",
            ]
        )

    for p in candidates:
        if p.is_file():
            return p
    return None


def _prompt_yes_no(msg: str, default_yes: bool = True) -> bool:
    suffix = "[Y/n]" if default_yes else "[y/N]"
    raw = input(f"{msg} {suffix} ").strip().lower()
    if not raw:
        return default_yes
    return raw in {"y", "yes"}


def _extract_archive(archive_path: Path, install_dir: Path) -> None:
    install_dir.mkdir(parents=True, exist_ok=True)
    lower = archive_path.name.lower()
    if lower.endswith(".zip"):
        with zipfile.ZipFile(archive_path) as zf:
            zf.extractall(install_dir)
        return
    if lower.endswith(".tar.xz") or lower.endswith(".tar.gz") or lower.endswith(".tgz"):
        with tar_open(archive_path) as tf:
            tf.extractall(install_dir)
        return
    raise ValueError(f"Unsupported archive format: {archive_path.name}")


def _search_for_opera_binary(root: Path) -> Path | None:
    names = ["OPERA.exe", "OPERA_P.exe"] if _is_windows() else ["OPERA", "opera"]
    for n in names:
        for p in root.rglob(n):
            if p.is_file():
                return p
    return None


def _upsert_env_var(path: Path, key: str, value: str) -> None:
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
    updated = False
    new_lines: list[str] = []
    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        if new_lines and new_lines[-1].strip():
            new_lines.append("")
        new_lines.append(f"{key}={value}")
    path.write_text("\n".join(new_lines).rstrip() + "\n", encoding="utf-8")


def _healthcheck(binary: Path) -> bool:
    # OPERA builds do not always support --version. Existence and executable-bit check is enough here.
    if not binary.is_file():
        return False
    if _is_windows():
        return binary.suffix.lower() == ".exe"
    return os.access(binary, os.X_OK)


def _install_from_release(install_dir: Path, non_interactive: bool) -> Path | None:
    releases = _request_json(GITHUB_API_RELEASES)
    if not releases:
        print("[warn] Could not query GitHub releases.")
        return None
    if isinstance(releases, dict) and releases.get("message"):
        print(f"[warn] GitHub API message: {releases.get('message')}")
        return None
    if not isinstance(releases, list) or not releases:
        print("[warn] No releases found.")
        return None

    latest = releases[0]
    tag = latest.get("tag_name") or "latest"
    asset = _pick_asset(latest)
    if not asset:
        print("[warn] No compatible release asset found automatically.")
        print(f"[info] Download manually: {RELEASES_PAGE}")
        return None

    asset_name = str(asset.get("name", ""))
    asset_url = str(asset.get("browser_download_url", ""))
    if not asset_url:
        print("[warn] Release asset does not include a download URL.")
        return None

    print(f"[info] Latest release: {tag}")
    print(f"[info] Selected asset: {asset_name}")
    if not non_interactive and not _prompt_yes_no("Download and install this asset?", default_yes=True):
        print("[info] Installation canceled by user.")
        return None

    tmp_dir = Path(tempfile.mkdtemp(prefix="hazquery_opera_setup_"))
    try:
        download_name = Path(urlparse(asset_url).path).name or asset_name
        download_path = tmp_dir / download_name
        print(f"[info] Downloading: {asset_url}")
        _download_file(asset_url, download_path)
        print(f"[info] Downloaded to: {download_path}")

        if download_path.suffix.lower() in {".msi", ".exe"}:
            print("[warn] Installer package detected (.msi/.exe).")
            print("[warn] Silent flags vary by OPERA build; running unattended may fail.")
            print("[info] Please install manually, then re-run this script with --skip-download.")
            return None

        _extract_archive(download_path, install_dir)
        exe = _search_for_opera_binary(install_dir)
        if exe:
            return exe
        print("[warn] Archive extracted, but OPERA binary was not found automatically.")
        return None
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Setup local OPERA and write HAZQUERY_OPERA_EXE into .env")
    p.add_argument("--install-dir", default=str(DEFAULT_OPERA_DIR), help="Directory for OPERA assets")
    p.add_argument("--yes", action="store_true", help="Non-interactive mode (auto-accept prompts)")
    p.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip GitHub download and only detect OPERA in existing locations/install-dir",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    install_dir = Path(args.install_dir).expanduser().resolve()

    existing = _discover_existing_opera()
    if existing:
        print(f"[ok] Found existing OPERA binary: {existing}")
        _upsert_env_var(ENV_FILE, "HAZQUERY_OPERA_EXE", str(existing))
        print(f"[ok] Updated {ENV_FILE} with HAZQUERY_OPERA_EXE")
        return 0

    print("[info] OPERA executable not found in standard locations.")
    if args.skip_download:
        print("[warn] --skip-download enabled and no binary found.")
        print(f"[next] Manually install OPERA from: {RELEASES_PAGE}")
        print("[next] Then set HAZQUERY_OPERA_EXE in .env")
        return 1

    if not args.yes and not _prompt_yes_no("Install OPERA now from the latest GitHub release?", default_yes=True):
        print("[info] Skipped installation.")
        print(f"[next] Manual download: {RELEASES_PAGE}")
        return 1

    install_dir.mkdir(parents=True, exist_ok=True)
    installed = _install_from_release(install_dir=install_dir, non_interactive=args.yes)
    if not installed:
        print("[warn] Automatic install could not complete.")
        print(f"[next] Install manually from: {RELEASES_PAGE}")
        print("[next] Then set HAZQUERY_OPERA_EXE in .env to your OPERA executable path.")
        return 1

    if not _healthcheck(installed):
        print(f"[warn] Binary found but healthcheck failed: {installed}")
        return 1

    _upsert_env_var(ENV_FILE, "HAZQUERY_OPERA_EXE", str(installed))
    _upsert_env_var(ENV_FILE, "OPERA_INSTALL_DIR", str(install_dir))
    print(f"[ok] OPERA configured at: {installed}")
    print(f"[ok] Wrote HAZQUERY_OPERA_EXE and OPERA_INSTALL_DIR to {ENV_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
