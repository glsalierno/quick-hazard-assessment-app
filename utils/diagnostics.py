"""Deployment diagnostics that do not expose absolute paths or credentials."""

from __future__ import annotations

import logging
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

logger = logging.getLogger(__name__)
_LFS_HEADER = b"version https://git-lfs.github.com/spec/v1"


def artifact_status(path: Path) -> str:
    """Identify missing artifacts and unfetched Git LFS pointers."""
    try:
        with path.open("rb") as handle:
            header = handle.readline(128).rstrip(b"\r\n")
    except FileNotFoundError:
        return "missing"
    except OSError as error:
        logger.warning("Cannot read bundled artifact %s (%s)", path.name, type(error).__name__)
        return "unreadable"
    return "Git LFS pointer (data not downloaded)" if header == _LFS_HEADER else "present"


def package_version(name: str) -> str:
    """Return an installed package's version, or a human-readable fallback."""
    try:
        return version(name)
    except PackageNotFoundError:
        return "not installed"
