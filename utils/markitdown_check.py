"""
MarkItDown is required for v1.4 SDS PDF CAS extraction (see docs/SDS_EXTRACTION_PIPELINES.md).
Without it, hybrid / markitdown_fast pipelines return no CAS.
"""

from __future__ import annotations

from typing import Optional, Tuple

# PyPI (default). Fork for source/contrib: https://github.com/glsalierno/markitdown
_INSTALL_PYPI = "pip install 'markitdown[pdf]'"
_INSTALL_GIT = (
    'pip install "markitdown[pdf] @ git+https://github.com/glsalierno/markitdown.git'
    '#subdirectory=packages/markitdown"'
)
_PUBLIC_MSG = (
    "MarkItDown is required for SDS PDF parsing (PDF → Markdown → CAS regex). "
    f"Install: `{_INSTALL_PYPI}` "
    "(or from GitHub [glsalierno/markitdown](https://github.com/glsalierno/markitdown): "
    f"`{_INSTALL_GIT}`)"
)


def is_markitdown_available() -> Tuple[bool, Optional[str]]:
    """
    Return (True, None) if ``markitdown`` imports.
    Otherwise (False, message suitable for UI or logs).
    """
    try:
        from markitdown import MarkItDown  # noqa: F401
    except ImportError as e:
        return False, f"{_PUBLIC_MSG} — ({e})"
    return True, None


def require_markitdown() -> None:
    """Raise RuntimeError if MarkItDown is missing; use before any SDS PDF conversion."""
    ok, err = is_markitdown_available()
    if not ok:
        raise RuntimeError(err or _PUBLIC_MSG)
