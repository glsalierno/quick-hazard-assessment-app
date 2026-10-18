"""Persist local GHaz7 settings (HSPiP install path, etc.)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import config

_DEFAULT_KEYS = (
    "hspip_install_dir",
    "hspip_cli_script",
)


def settings_path() -> Path:
    raw = getattr(config, "V7_SETTINGS_JSON", None)
    if raw:
        return Path(str(raw))
    return Path(getattr(config, "DATA_DIR", "data")) / "ghaz7_settings.json"


def load_settings() -> dict[str, Any]:
    path = settings_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_settings(updates: dict[str, Any]) -> dict[str, Any]:
    path = settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    current = load_settings()
    for key, value in updates.items():
        if value is None or value == "":
            current.pop(key, None)
        else:
            current[key] = value
    path.write_text(json.dumps(current, indent=2), encoding="utf-8")
    return current


def get_hspip_install_dir() -> str | None:
    env = (getattr(config, "HSPIP_INSTALL_DIR", None) or "").strip()
    if env and Path(env).is_dir():
        return env
    stored = str(load_settings().get("hspip_install_dir") or "").strip()
    if stored and Path(stored).is_dir():
        return stored
    # Common Windows defaults (may not exist)
    for candidate in (
        r"C:\Program Files\Hansen-Solubility-6\HSPiP",
        r"D:\Program Files\Hansen-Solubility\HSPiP",
        r"C:\Program Files\Hansen-Solubility\HSPiP",
        r"C:\Program Files (x86)\Hansen-Solubility\HSPiP",
    ):
        if (Path(candidate) / "HSPiP.exe").is_file():
            return candidate
    return stored or env or None


def set_hspip_install_dir(path: str) -> str:
    p = Path(path.strip().strip('"'))
    if not p.is_dir():
        raise ValueError(f"HSPiP directory not found: {p}")
    exe = p / "HSPiP.exe"
    if not exe.is_file():
        raise ValueError(f"HSPiP.exe not found in {p}")
    save_settings({"hspip_install_dir": str(p)})
    return str(p)
