"""
PDF extraction cache: fast fingerprint, per-artifact files, TTL invalidation.

Layout (under cache root):
  {sha256_64}/
    markitdown.md
    tables.json
    ocr_text.json
    cas_results.json
    meta.json          # version, timestamps
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Bump when on-disk format changes
CACHE_FORMAT_VERSION = "1"


def pdf_fingerprint(pdf_bytes: bytes) -> str:
    """SHA256 of first 1MB (fast); full hex digest for low collision risk."""
    if not pdf_bytes:
        return hashlib.sha256(b"").hexdigest()
    chunk = pdf_bytes[: 1024 * 1024]
    return hashlib.sha256(chunk).hexdigest()


class ExtractionCacheManager:
    """
    Disk cache for alternative extraction pipelines (MarkItDown / OCR / CAS results).
    """

    def __init__(
        self,
        cache_root: Optional[Path] = None,
        *,
        max_age_days: float = 30.0,
        format_version: str = CACHE_FORMAT_VERSION,
    ) -> None:
        self.cache_root = Path(cache_root) if cache_root else Path(__file__).resolve().parent.parent / "cache"
        self.max_age_days = max_age_days
        self.format_version = format_version

    def _entry_dir(self, fingerprint: str) -> Path:
        return self.cache_root / fingerprint

    def ensure_root(self) -> None:
        self.cache_root.mkdir(parents=True, exist_ok=True)

    def is_stale(self, fingerprint: str) -> bool:
        """True if meta is missing, version mismatch, or older than max_age_days."""
        meta_path = self._entry_dir(fingerprint) / "meta.json"
        if not meta_path.is_file():
            return True
        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            return True
        if str(meta.get("version")) != str(self.format_version):
            return True
        ts = float(meta.get("updated_ts", 0))
        if ts <= 0:
            return True
        age_sec = time.time() - ts
        return age_sec > self.max_age_days * 86400.0

    def load_text(self, fingerprint: str, name: str) -> Optional[str]:
        p = self._entry_dir(fingerprint) / name
        if not p.is_file():
            return None
        if self.is_stale(fingerprint):
            return None
        try:
            return p.read_text(encoding="utf-8")
        except Exception as e:
            logger.debug("load_text failed %s: %s", p, e)
            return None

    def save_text(self, fingerprint: str, name: str, content: str) -> None:
        d = self._entry_dir(fingerprint)
        d.mkdir(parents=True, exist_ok=True)
        (d / name).write_text(content or "", encoding="utf-8")
        self._touch_meta(fingerprint)

    def load_json(self, fingerprint: str, name: str) -> Optional[Any]:
        p = self._entry_dir(fingerprint) / name
        if not p.is_file():
            return None
        if self.is_stale(fingerprint):
            return None
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.debug("load_json failed %s: %s", p, e)
            return None

    def save_json(self, fingerprint: str, name: str, data: Any) -> None:
        d = self._entry_dir(fingerprint)
        d.mkdir(parents=True, exist_ok=True)
        with open(d / name, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        self._touch_meta(fingerprint)

    def _touch_meta(self, fingerprint: str) -> None:
        d = self._entry_dir(fingerprint)
        d.mkdir(parents=True, exist_ok=True)
        meta_path = d / "meta.json"
        prev: dict[str, Any] = {}
        if meta_path.is_file():
            try:
                with open(meta_path, encoding="utf-8") as f:
                    prev = json.load(f)
            except Exception:
                prev = {}
        prev.update(
            {
                "version": self.format_version,
                "updated_ts": time.time(),
            }
        )
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(prev, f, indent=2)

    def cleanup_expired(self) -> int:
        """Remove cache entries older than max_age_days or wrong version. Returns count removed."""
        self.ensure_root()
        removed = 0
        now = time.time()
        for child in self.cache_root.iterdir():
            if not child.is_dir():
                continue
            meta = child / "meta.json"
            stale = True
            if meta.is_file():
                try:
                    with open(meta, encoding="utf-8") as f:
                        m = json.load(f)
                    ts = float(m.get("updated_ts", 0))
                    ver_ok = str(m.get("version")) == str(self.format_version)
                    stale = (not ver_ok) or (now - ts > self.max_age_days * 86400.0)
                except Exception:
                    stale = True
            if stale:
                try:
                    import shutil

                    shutil.rmtree(child, ignore_errors=True)
                    removed += 1
                except Exception:
                    pass
        return removed

    def clear_all(self) -> None:
        """Delete entire cache root contents."""
        if not self.cache_root.is_dir():
            return
        import shutil

        for child in list(self.cache_root.iterdir()):
            try:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
            except Exception:
                pass


def default_cache_manager() -> ExtractionCacheManager:
    return ExtractionCacheManager()
